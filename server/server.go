package main

import (
	"encoding/binary"
	"fmt"
	"golang.org/x/sys/unix"
	"log"
	"syscall"
)

const (
	SERVER_PORT         = 9988
	messageLimit        = 4096
	messageHeaderLength = 4
)

func main() {
	fd, _ := syscall.Socket(syscall.AF_INET, syscall.O_NONBLOCK|syscall.SOCK_STREAM, 0)
	syscall.Bind(fd, &syscall.SockaddrInet4{
		Port: SERVER_PORT,
		Addr: [4]byte{127, 0, 0, 1},
	})
	err := syscall.Listen(fd, 1024)
	if err != nil {
		log.Fatal(err)
	}
	defer syscall.Close(fd)
	log.Println("Waiting for client...")
	connections:=make( map[int32]*Connection)
	var events []unix.PollFd
	for {
		events = nil
		pfd := unix.PollFd{
			Fd:      int32(fd),
			Events:  unix.POLLIN,
			Revents: 0,
		}
		events = append(events, pfd)
		for _, conn := range connections {
			if conn == nil {
				continue
			}
			var pfd unix.PollFd
			pfd.Fd = int32(conn.fd)
			if conn.state == STATE_REQ {
				pfd.Events = unix.POLLIN
			} else {
				pfd.Events = unix.POLLOUT
			}
			pfd.Events = pfd.Events | unix.POLLERR
			events = append(events, pfd)
		}
		n, err := unix.Poll(events, -1)
		if err != nil {
			log.Fatal(err)
		}
		if n > 0 {
			for _, event := range events {
				if event.Revents != 0 {
					elem, ok := connections[event.Fd] 
					var conn *Connection
					if ok {
						conn = elem
					} else {
						continue
					}
					ConnectionIO(conn)
					if conn.state == STATE_END {
						log.Printf("connection %d deleted from event list\n", conn.fd)
						delete(connections, event.Fd)
						syscall.Close(conn.fd)
					}
				}
			}
		}
		if events[0].Revents !=0 {
			log.Println("new connection!!!")
			acceptNewConnection(connections, fd)
		}
	}
}





func tryOneRequest(conn *Connection) bool {
	if conn.readBufferSize < messageHeaderLength {
		return false
	}
	length := binary.LittleEndian.Uint32(conn.readBuffer[:messageHeaderLength])
	if length > messageLimit {
		log.Println("message too long")
		conn.state = STATE_END
		return false
	}
	if (messageHeaderLength + length > uint32(conn.readBufferSize)){
		return false
	}
	fmt.Printf("Client says: %s\n", string(conn.readBuffer[messageHeaderLength:]))
	binary.LittleEndian.PutUint32(conn.writeBuffer[:messageHeaderLength], length)
    copy(conn.writeBuffer[messageHeaderLength:messageHeaderLength+length], conn.readBuffer[messageHeaderLength:messageHeaderLength+length])
    conn.writeBufferSize = messageHeaderLength + uint(length)
	remain:= conn.readBufferSize - messageHeaderLength - uint(length)
	if remain>0 {
		copy(conn.readBuffer[:remain], conn.readBuffer[messageHeaderLength+length: messageHeaderLength + uint(length)+remain])
	}
	conn.readBufferSize = remain
	conn.state = STATE_RES
	StateRes(conn)
	return conn.state == STATE_REQ
}