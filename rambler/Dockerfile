FROM golang:1.10-stretch

RUN go get github.com/elwinar/rambler

COPY migrations migrations
COPY rambler.json ./
COPY apply-all.sh ./

CMD ./apply-all.sh
