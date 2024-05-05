FROM golang:1.21.9-alpine3.19

WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o main .
CMD [ "./main" ]