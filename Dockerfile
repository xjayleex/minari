FROM golang:1.20-buster as gobuilder

WORKDIR /shipperapp

COPY . .

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-s -w' -o main main.go

FROM scratch
COPY --from=gobuilder /shipperapp /

ENV APP_ENV production
ENTRYPOINT ["/main"]