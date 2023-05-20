package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
)

const (
	SERVER_HOST         = "localhost"
	SERVER_PORT_C       = "9988"
	SERVER_TYPE         = "tcp"
	messageLimit        = 4096
	messageHeaderLength = 4
)

const (
	SER_NIL int = iota
	SER_ERR
	SER_STR
	SER_INT
	SER_ARR
)

func main() {

	flag.Parse()
	connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT_C)
	if err != nil {
		panic(err)
	}
	if err := sendRequest(connection, flag.Args()); err != nil {
		fmt.Println(err)
	}
	if err := readResponse(connection); err != nil {
		fmt.Println(err)
	}
	defer connection.Close()

}

func writeAll(connection net.Conn, buffer []byte, length uint32) error {

	for length > 0 {
		rv, err := connection.Write(buffer[:length])
		if err != nil || rv <= 0 {
			return err
		}
		length -= uint32(rv)
		buffer = buffer[rv:]
	}
	return nil
}

func readFull(connection net.Conn, buffer []byte, length uint32) error {

	for length > 0 {
		rv, err := connection.Read(buffer[:length])
		if err != nil || rv < 0 {
			return err
		}
		if rv == 0 {
			return errors.New("EOF")
		}
		if uint32(rv) > length {
			return errors.New("more than byte read from connection")
		}
		length -= uint32(rv)
		buffer = buffer[rv:]
	}
	return nil

}

func sendRequest(connection net.Conn, cmd []string) error {

	var length uint32 = 4
	for _, s := range cmd {
		length += 4 + uint32(len(s))
	}
	if length > messageLimit {
		return errors.New("message too large")
	}

	buf := make([]byte, messageHeaderLength+messageLimit)
	binary.LittleEndian.PutUint32(buf[:4], length)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(cmd)))

	cur := 8
	for _, s := range cmd {
		p := uint32(len(s))
		binary.LittleEndian.PutUint32(buf[cur:cur+4], p)
		copy(buf[cur+4:cur+4+int(p)], []byte(s))
		cur += 4 + int(p)
	}
	return writeAll(connection, buf, 4+length)

}

func readResponse(conn net.Conn) error {
	rbuf := make([]byte, messageHeaderLength+messageLimit)
	if err := readFull(conn, rbuf, messageHeaderLength); err != nil {
		return err
	}
	length := binary.LittleEndian.Uint32(rbuf[:messageHeaderLength])
	if length > messageLimit {
		return errors.New("message too large")
	}
	if err := readFull(conn, rbuf[messageHeaderLength:], length); err != nil {
		return err
	}
	
	rv:=onResponse(rbuf[4:], int(length))
	if rv>0 && rv!=int32(length) {
		return errors.New("bad respose")
	}
	return nil
}

func onResponse(data []byte, size int) int32 {
	if size < 1 {
		log.Println("bad response")
		return int32(-1)
	}

	switch int(data[0]) {

	case SER_NIL:
		log.Println("nil")
		return int32(1)

	case SER_ERR:
		if size < 1+8 {
			log.Println("bad response")
			return int32(-1)
		}

		var code int32 = 0
		var length uint32 = 0

		code = int32(binary.LittleEndian.Uint32(data[1:1+4]))
		length = binary.LittleEndian.Uint32(data[5 : 5+4])


		if size < 1+8+int(length) {
			log.Println("bad response")
			return int32(-1)
		}
		fmt.Printf("(err) code:[%v] len:[%v] response:%s\n", code, length, string(data[1+8:1+8+length]))
		return int32(1 + 8 + length)

	case SER_STR:
		
		if size < 1+4 {
			log.Println("bad response")
			return int32(-1)
		}
		var len uint32 = 0
		len = binary.LittleEndian.Uint32(data[1 : 1+4])
		if size < 1+4+int(len) {
			log.Println("bad response")
			return int32(-1)
		}
		fmt.Printf("(str) len:[%d] response:%s\n", len, string(data[1+4:1+4+int(len)]));
        return int32(1 + 4 + len);


	case SER_INT:
		if size < 1+8 {
			fmt.Println("bad response")
			return int32(-1)
		}

		var val int64 = 0
		val = int64(binary.LittleEndian.Uint64(data[1 : 1+8]))
		fmt.Printf("(int) val:[%d]\n", val)
		return int32(1 + 8)

	case SER_ARR:
		if size < 1+4 {
			fmt.Println("bad response")
			return int32(-1)
		}
		var len uint32 = 0
		len = binary.LittleEndian.Uint32(data[1 : 1+4])
		fmt.Printf("(arr) len:[%d]\n", len)

		pos:=1+4
		for i:=0;i<int(len);i++{
			rv:=onResponse(data[pos:],size-pos)
			if rv<0{
				return rv
			}
			pos += int(rv)
		}
		fmt.Printf("(arr) end\n")
		return int32(pos)
		
	default:
		fmt.Println("bad response")
		return int32(-1)
	}

}
