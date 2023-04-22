package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	SERVER_PORT         = 9988
	messageLimit        = 4096
	messageHeaderLength = 4
)

type ResponseCode int

const (
	RES_OK ResponseCode = iota
	RES_ERR
	RES_NX
)

var gMap = make(map[string]string) 

func main() {
	fd, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
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
	connections := make(map[int32]*Connection)
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
		log.Println("Polling for connections....")
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
		if events[0].Revents != 0 {
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
	if messageHeaderLength+length > uint32(conn.readBufferSize) {
		return false
	}

	var responseCode int = 0
	var responseLenght uint32 = 0


	if err := doRequest(conn.readBuffer[messageHeaderLength:], length, &responseCode, conn.writeBuffer[messageHeaderLength+4:], &responseLenght); err != nil {
		log.Println(err)
		conn.state = STATE_END
		return false
	}

	responseLenght += messageHeaderLength
	binary.LittleEndian.PutUint32(conn.writeBuffer[:messageHeaderLength], responseLenght)
	binary.LittleEndian.PutUint32(conn.writeBuffer[messageHeaderLength:messageHeaderLength+4], uint32(responseCode))
	conn.writeBufferSize = uint(responseLenght) + 4

	remain := conn.readBufferSize - messageHeaderLength - uint(length)
	if remain > 0 {
		copy(conn.readBuffer[:remain], conn.readBuffer[messageHeaderLength+length:messageHeaderLength+uint(length)+remain])
	}
	conn.readBufferSize = remain
	conn.state = STATE_RES
	StateRes(conn)
	return conn.state == STATE_REQ
}

func doRequest(request []byte, requestLength uint32, responseCode *int, response []byte, responseLenght *uint32) error {
	cmd := make([]string, 0)
	if err := parseRequest(request, requestLength, &cmd); err != nil {
		log.Println("bad request!", err)
		return err
	}
	if len(cmd) == 2 && cmd[0] == "get" {
		*responseCode = doGet(cmd, response, responseLenght)
	} else if len(cmd) == 3 && cmd[0] == "set" {
		*responseCode = doSet(cmd, response, responseLenght)
	} else if len(cmd) == 2 && cmd[0] == "del" {
		*responseCode = doDel(cmd, response, responseLenght)
	} else {
		*responseCode = int(RES_ERR)
		message := "Unknown command"
		copy(response, []byte(message))
		*responseLenght = uint32(len(message))
		return nil
	}
	return nil
}

func parseRequest(request []byte, requestLength uint32, cmd *[]string) error {

	if requestLength<=4{
		return errors.New("empty request")
	}


	length := binary.LittleEndian.Uint32(request[:messageHeaderLength])
	
	var position uint32 = 4
	for length > 0 {
		
		if position+4 > requestLength {
			return errors.New("error when trying to parse message")
		}
		sz := binary.LittleEndian.Uint32(request[position : position+4])
		if position+4+sz > requestLength {
			return errors.New("error when trying to parse message")
		}
		*cmd = append(*cmd, string(request[position+4:position+4+sz]))
		position += 4 + sz
		length -= 1
	}

	if position != requestLength {
		return errors.New("trailing garbage after message")
	}
	return nil
}

func doGet(cmd []string, response []byte, responseLength *uint32) int {
	item, ok := gMap[cmd[1]]
	if !ok {
		return int(RES_NX)
	}
	if len(item) > messageLimit {
		log.Println("Message too long")
	}
	copy(response, []byte(item))
	*responseLength = uint32(len(item))
	return int(RES_OK)
}

func doSet(cmd []string, response []byte, responseLength *uint32) int {
	gMap[cmd[1]] = cmd[2]
	fmt.Println("gmap:", gMap)
	return int(RES_OK)
}

func doDel(cmd []string, response []byte, responseLength *uint32) int {
	delete(gMap, cmd[1])
	return int(RES_OK)
}
