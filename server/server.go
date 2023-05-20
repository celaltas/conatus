package main

import (
	"encoding/binary"
	"errors"
	"log"
	"syscall"

	"golang.org/x/sys/unix"
)

type ResponseCode int
type SerializationCode int
type ErrorTypeCode int32

const (
	SERVER_PORT           = 9988
	MESSAGE_LIMIT         = 4096
	MESSAGE_HEADER_LENGTH = 4
)

const (
	RES_OK ResponseCode = iota
	RES_ERR
	RES_NX
)

const (
	SER_NIL SerializationCode = iota
	SER_ERR
	SER_STR
	SER_INT
	SER_ARR
)

const (
	ERR_UNKNOWN ErrorTypeCode = iota
	ERR_2BIG
)

var gmap *gMap

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
	gmap = InitMap()

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
	if conn.readBufferSize < MESSAGE_HEADER_LENGTH {
		return false
	}
	length := binary.LittleEndian.Uint32(conn.readBuffer[:MESSAGE_HEADER_LENGTH])
	if length > MESSAGE_LIMIT {
		log.Println("message too long")
		conn.state = STATE_END
		return false
	}
	if MESSAGE_HEADER_LENGTH+length > uint32(conn.readBufferSize) {
		return false
	}
	cmd := make([]string, 0)
	if err := parseRequest(conn.readBuffer[MESSAGE_HEADER_LENGTH:], length, &cmd); err != nil {
		log.Println(err)
		conn.state = STATE_END
		return false
	}

	var out string
	doRequest(&cmd, &out)

	if MESSAGE_HEADER_LENGTH+len(out) > MESSAGE_LIMIT {
		out = ""
		outErr(&out, ERR_2BIG, "response is too big")
	}
	binary.LittleEndian.PutUint32(conn.writeBuffer[:MESSAGE_HEADER_LENGTH], (uint32)(len(out)))
	copy(conn.writeBuffer[MESSAGE_HEADER_LENGTH:MESSAGE_HEADER_LENGTH+len(out)], []byte(out))
	conn.writeBufferSize = uint(len(out)) + 4
	remain := conn.readBufferSize - MESSAGE_HEADER_LENGTH - uint(length)
	if remain > 0 {
		copy(conn.readBuffer[:remain], conn.readBuffer[MESSAGE_HEADER_LENGTH+length:MESSAGE_HEADER_LENGTH+uint(length)+remain])
	}



	conn.readBufferSize = remain
	conn.state = STATE_RES
	StateRes(conn)
	return conn.state == STATE_REQ
}

func doRequest(cmd *[]string, out *string) {



	if len(*cmd) == 1 && (*cmd)[0] == "keys" {
		doKeys(*cmd, out)
	} else if len(*cmd) == 2 && (*cmd)[0] == "get" {
		doGet(*cmd, out)
	} else if len(*cmd) == 3 && (*cmd)[0] == "set" {
		doSet(*cmd, out)
	} else if len(*cmd) == 2 && (*cmd)[0] == "del" {
		doDel(*cmd, out)
	} else {
		outErr(out, ERR_UNKNOWN, "Unknown command")
	}
}

func parseRequest(request []byte, requestLength uint32, cmd *[]string) error {
	if requestLength <= 4 {
		return errors.New("empty request")
	}
	length := binary.LittleEndian.Uint32(request[:MESSAGE_HEADER_LENGTH])
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

func doKeys(cmd []string, out *string) {
	outArr(out, uint32(gmap.db.hmapSize()))
	gmap.db.firstTab.hmapScan(cbScan, out)
	gmap.db.secondTab.hmapScan(cbScan, out)
}

func doGet(cmd []string, out *string) {
	node := newNode(cmd[1], "")
	found := gmap.db.lookupNode(node, nodeComparer)
	if found == nil {
		outNil(out)
	} else {
		outStr(out, found.value)
	}

}

func doSet(cmd []string, out *string) {

	node := newNode(cmd[1], "")
	found := gmap.db.lookupNode(node, nodeComparer)
	if found != nil {
		found.value = cmd[2]
	} else {
		gmap.db.insert(newNode(cmd[1], cmd[2]))
	}
	outNil(out)
}

func doDel(cmd []string, out *string) {
	node := newNode(cmd[1], "")
	deletedNode := gmap.db.pop(node, nodeComparer)
	if deletedNode != nil {
		outInt(out, uint64(1))
	} else {
		outInt(out, uint64(0))
	}

}

func outNil(out *string) {
	*out = string([]byte{byte(SER_NIL)})
}

func outStr(out *string, val string) {
	*out += string([]byte{byte(SER_STR)})
	buff := make([]byte, 4)
	binary.LittleEndian.PutUint32(buff, uint32(len(val)))
	*out += string(buff)
	*out += val
}

func outInt(out *string, val uint64) {
	*out += string([]byte{byte(SER_INT)})
	buff := make([]byte, 8)
	binary.LittleEndian.PutUint64(buff, val)
	*out += string(buff)

}

func outErr(out *string, code ErrorTypeCode, message string) {
	*out += string([]byte{byte(SER_ERR)})
	buff := make([]byte, 8)
	binary.LittleEndian.PutUint32(buff[:4], uint32(code))
	binary.LittleEndian.PutUint32(buff[4:], uint32(len(message)))
	*out += string(buff)
	*out += message

}

func outArr(out *string, n uint32) {
	
	*out += string([]byte{byte(SER_ARR)})
	buff := make([]byte, 4)
	binary.LittleEndian.PutUint32(buff, n)
	*out += string(buff)

}
