FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/limiterd ./cmd/limiterd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/limiterd /limiterd
COPY configs /configs
EXPOSE 8080
ENTRYPOINT ["/limiterd"]
CMD ["-config=/configs/rules.json"]
