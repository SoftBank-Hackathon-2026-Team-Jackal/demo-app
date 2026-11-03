# syntax=docker/dockerfile:1.7
# 로컬 Docker Compose용 빌드. 레지스트리에 올리는 멀티 아키텍처 이미지는 ko로 만든다 (.ko.yaml)
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY *.go ./
COPY static ./static
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/demo-app .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/demo-app /demo-app
EXPOSE 8080
ENTRYPOINT ["/demo-app"]
