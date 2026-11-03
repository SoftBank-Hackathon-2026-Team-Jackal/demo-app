"""데모 채팅방에서 계속 대화하는 AI 에이전트.

에이전트는 실제 사용자와 같은 경로(로드 밸런서 → 앱 → PgBouncer → DB)로만 채팅방에 접속한다.
그래서 이전하는 동안 메시지가 빠지거나 대화가 끊기면, 에이전트가 그 사실을 알아채고 말한다.

    uv run chatbots.py --url http://localhost:8080            # Claude로 대화
    uv run chatbots.py --url http://localhost:8080 --offline  # API 없이 준비된 대사로 대화

종료(Ctrl-C)하면 보낸 메시지, 저장 확인, 스트림 수신, 유실, 중복 집계를 출력한다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import secrets
import signal
import time
from collections import Counter
from dataclasses import dataclass, field

import anthropic
import httpx

log = logging.getLogger("chatbots")

MODEL = os.environ.get("CHAT_MODEL", "claude-haiku-5-5")
HISTORY_WINDOW = 20  # 프롬프트에 넣는 최근 메시지 수
MISSING_AFTER_S = 10.0  # 저장 확인 후 이 시간 안에 스트림에 안 보이면 유실 의심

COMMON_RULES = """\
너는 사내 채팅방에 있는 팀원이다. 팀은 지금 운영 중인 서비스를 다른 클라우드로 옮기는 중이다.
규칙:
- 한두 문장으로 짧게, 메신저 말투로 말한다. 이모지는 가끔만 쓴다.
- 다른 사람 말에 자연스럽게 반응하고, 가끔 질문도 던진다.
- [시스템 알림]에 환경이나 DB가 바뀌었다는 내용이 있으면 그 변화를 자연스럽게 언급한다.
- [시스템 알림]에 네 메시지가 사라졌다는 내용이 있으면 그 사실을 바로 지적한다.
- 이름표나 따옴표 없이 네가 할 말만 출력한다."""


@dataclass
class Persona:
    name: str
    role: str
    fallback: list[str]


PERSONAS = [
    Persona(
        "SRE봇",
        "걱정 많은 SRE. 지연 시간, 롤백 계획, 장애 가능성을 늘 신경 쓴다.",
        ["지연 그래프 보는 중인데 아직 괜찮네요", "롤백 경로는 살아 있죠?", "CDC 지연 0 유지 중입니다"],
    ),
    Persona(
        "PM봇",
        "낙관적인 PM. 일정과 사용자 영향을 챙기고 팀을 격려한다.",
        ["사용자 문의는 아직 0건이에요 👍", "이대로면 오늘 안에 끝나겠네요", "다들 고생 많아요!"],
    ),
    Persona(
        "보안봇",
        "의심 많은 보안 담당자. 공개 포트, 권한, 암호화를 꼼꼼히 따진다.",
        ["새 환경 DB는 사설망에서만 열려 있는 거 맞죠?", "시크릿이 평문으로 남은 곳은 없나요", "감사 로그도 같이 넘어가야 해요"],
    ),
    Persona(
        "ケンジ",
        "일본 지사에서 참여한 개발자. 항상 일본어로 짧게 말한다.",
        ["こちらも問題なく動いています", "移行、順調ですね", "ログを確認しました、エラーなしです"],
    ),
]


def new_id() -> str:
    return secrets.token_hex(16)


@dataclass
class Sent:
    persona: str
    body: str
    sent_at: float
    acked_at: float | None = None
    msg_id: int | None = None
    reported_missing: bool = False


@dataclass
class Room:
    base_url: str
    history: list[dict] = field(default_factory=list)
    last_id: int = 0
    ledger: dict[str, Sent] = field(default_factory=dict)
    seen_counts: Counter = field(default_factory=Counter)  # client_msg_id -> 스트림에서 받은 횟수
    notes: dict[str, list[str]] = field(default_factory=dict)  # persona -> 다음 발언 때 전달할 알림
    last_route: dict[str, tuple[str, str]] = field(default_factory=dict)  # persona -> (app_env, db_node)
    reconnects: int = 0
    stream_status: str = "-"

    def note(self, persona: str, text: str) -> None:
        self.notes.setdefault(persona, []).append(text)

    # ---- 수신: SSE ----
    async def listen(self, http: httpx.AsyncClient) -> None:
        while True:
            headers = {"Accept": "text/event-stream"}
            if self.last_id:
                headers["Last-Event-ID"] = str(self.last_id)
            try:
                async with http.stream("GET", f"{self.base_url}/api/stream", headers=headers, timeout=httpx.Timeout(10, read=30)) as resp:
                    if resp.status_code != 200:
                        raise httpx.HTTPStatusError("stream", request=resp.request, response=resp)
                    event, data = "message", []
                    async for line in resp.aiter_lines():
                        if line == "":
                            if data:
                                self._on_event(event, "\n".join(data))
                            event, data = "message", []
                        elif line.startswith("event:"):
                            event = line[6:].strip()
                        elif line.startswith("data:"):
                            data.append(line[5:].lstrip())
            except (httpx.HTTPError, asyncio.TimeoutError) as e:
                log.info("stream disconnected: %s", type(e).__name__)
            self.reconnects += 1
            await asyncio.sleep(1)

    def _on_event(self, event: str, data: str) -> None:
        payload = json.loads(data)
        if event in ("hello", "status"):
            self.stream_status = f"{payload['app_env']} · {payload['db_node']}"
            return
        if event != "message":
            return
        m = payload
        if m["id"] <= self.last_id:
            return
        self.last_id = m["id"]
        self.history.append(m)
        del self.history[:-200]
        self.seen_counts[m["client_msg_id"]] += 1
        print(f"[{m['app_env']} · {m['db_node']}] {m['author']}: {m['body']}", flush=True)

    # ---- 전송: 성공할 때까지 같은 client_msg_id로 재전송 ----
    async def post(self, http: httpx.AsyncClient, persona: str, body: str) -> None:
        cid = new_id()
        self.ledger[cid] = Sent(persona=persona, body=body, sent_at=time.monotonic())
        delay = 0.3
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                r = await http.post(
                    f"{self.base_url}/api/messages",
                    json={"client_msg_id": cid, "author": persona, "body": body},
                    timeout=35,
                )
                if r.status_code in (200, 201):
                    m = r.json()
                    s = self.ledger[cid]
                    s.acked_at, s.msg_id = time.monotonic(), m["id"]
                    self._track_route(persona, m["app_env"], m["db_node"])
                    return
                if 400 <= r.status_code < 500:
                    log.warning("rejected %s: %s", r.status_code, r.text)
                    return
            except httpx.HTTPError as e:
                log.info("post retry (%s)", type(e).__name__)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 3)
        log.error("gave up sending %s", cid)

    def _track_route(self, persona: str, app_env: str, db_node: str) -> None:
        prev = self.last_route.get(persona)
        self.last_route[persona] = (app_env, db_node)
        if prev and prev != (app_env, db_node):
            self.note(
                persona,
                f"방금 네 메시지는 {app_env} 환경의 앱이 처리했고 {db_node} DB에 저장됐다. "
                f"직전에는 {prev[0]} 앱, {prev[1]} DB였다.",
            )

    # ---- 유실 감시 ----
    async def watch_missing(self) -> None:
        while True:
            await asyncio.sleep(2)
            now = time.monotonic()
            for cid, s in self.ledger.items():
                if s.acked_at and not s.reported_missing and cid not in self.seen_counts and now - s.acked_at > MISSING_AFTER_S:
                    s.reported_missing = True
                    log.warning("MISSING: %s %r (id=%s)", s.persona, s.body, s.msg_id)
                    self.note(s.persona, f"네가 보낸 메시지 \"{s.body}\"가 저장됐다고 응답받았는데 채팅방에 보이지 않는다.")

    def summary(self) -> dict:
        acked = [c for c, s in self.ledger.items() if s.acked_at]
        seen = [c for c in self.ledger if c in self.seen_counts]
        return {
            "sent": len(self.ledger),
            "acked": len(acked),
            "seen_in_stream": len(seen),
            "missing_in_stream": len([c for c in acked if c not in self.seen_counts]),
            "duplicates": sum(1 for c in self.ledger if self.seen_counts[c] > 1),
            "stream_reconnects": self.reconnects,
        }


class Speaker:
    """다음에 말할 에이전트를 고르고, 할 말을 만든다."""

    def __init__(self, room: Room, offline: bool):
        self.room = room
        self.offline = offline
        self.client = None if offline else anthropic.AsyncAnthropic(max_retries=1, timeout=20)

    def pick(self) -> Persona:
        last = self.room.history[-1] if self.room.history else None
        if last:
            # 마지막 메시지에서 이름이 불린 에이전트가 먼저 답한다
            for p in PERSONAS:
                if p.name in last["body"] and p.name != last["author"]:
                    return p
        # 알림이 쌓인 에이전트가 있으면 먼저 말하게 한다
        for p in PERSONAS:
            if self.room.notes.get(p.name):
                return p
        candidates = [p for p in PERSONAS if not last or p.name != last["author"]]
        return random.choice(candidates)

    async def line_for(self, p: Persona) -> str:
        notes = self.room.notes.pop(p.name, [])
        if self.offline:
            if notes and "보이지 않는다" in " ".join(notes):
                return "어? 방금 내 메시지 어디 갔어요?"
            if notes:
                env, db = self.room.last_route[p.name]
                return f"오 방금 제 메시지는 {env} 앱 → {db} DB로 갔네요"
            return random.choice(p.fallback)

        transcript = "\n".join(
            f"[{m['app_env']} · {m['db_node']}] {m['author']}: {m['body']}" for m in self.room.history[-HISTORY_WINDOW:]
        ) or "(아직 대화가 없다. 네가 먼저 말을 꺼낸다.)"
        prompt = f"최근 채팅:\n{transcript}"
        if notes:
            prompt += "\n\n[시스템 알림]\n" + "\n".join(f"- {n}" for n in notes)
        prompt += f"\n\n너({p.name})의 다음 메시지를 써라."

        try:
            resp = await self.client.messages.create(
                model=MODEL,
                max_tokens=1024,
                output_config={"effort": "low"},
                system=f"너의 이름은 {p.name}. {p.role}\n\n{COMMON_RULES}",
                messages=[{"role": "user", "content": prompt}],
            )
        except anthropic.RateLimitError:
            log.warning("rate limited; using fallback line")
            return random.choice(p.fallback)
        except anthropic.APIStatusError as e:
            log.warning("api error %s; using fallback line", e.status_code)
            return random.choice(p.fallback)
        except anthropic.APIConnectionError:
            log.warning("api unreachable; using fallback line")
            return random.choice(p.fallback)

        if resp.stop_reason == "refusal":
            return random.choice(p.fallback)
        text = "".join(b.text for b in resp.content if b.type == "text").strip()
        return text[:500] or random.choice(p.fallback)


async def run(base_url: str, offline: bool, min_gap: float, max_gap: float) -> Room:
    room = Room(base_url=base_url.rstrip("/"))
    speaker = Speaker(room, offline)
    async with httpx.AsyncClient() as http:
        tasks = [asyncio.create_task(room.listen(http)), asyncio.create_task(room.watch_missing())]
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        await asyncio.sleep(1)  # 기존 대화를 먼저 받아 온다
        while not stop.is_set():
            p = speaker.pick()
            body = await speaker.line_for(p)
            await room.post(http, p.name, body)
            try:
                await asyncio.wait_for(stop.wait(), timeout=random.uniform(min_gap, max_gap))
            except asyncio.TimeoutError:
                pass

        # 마지막으로 보낸 메시지가 스트림에 도착할 시간을 준다
        await asyncio.sleep(2)
        for t in tasks:
            t.cancel()
    return room


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("CHAT_URL", "http://localhost:8080"))
    ap.add_argument("--offline", action="store_true", help="Claude API 없이 준비된 대사로 대화")
    ap.add_argument("--min-gap", type=float, default=3.0, help="발언 사이 최소 간격(초)")
    ap.add_argument("--max-gap", type=float, default=6.0, help="발언 사이 최대 간격(초)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    room = asyncio.run(run(args.url, args.offline, args.min_gap, args.max_gap))
    print(json.dumps(room.summary(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
