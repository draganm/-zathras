FROM golang:1.7.4-alpine
ADD . /go/src/github.com/draganm/zathras
WORKDIR /go/src/github.com/draganm/zathras
RUN go install .

