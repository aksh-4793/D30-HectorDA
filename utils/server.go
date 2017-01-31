package utils
import (
	"fmt"
	"net"
 	"github.com/aksh-4793/D30-HectorDA/proto_types/TransferOutMismatch"
 	"os"
 	"github.com/golang/protobuf/proto"
)


// used to start a TCP server
func Server () {
        fmt.Printf("Started ProtoBuf Server")
        fmt.Println("\nPort: 2110")
        c := make(chan *TransferOutMismatch.TransferOutMismatch)
        go func(){

                for{
                        message := <-c
                        Execute(message)
                }
        }()

        //Listen to the TCP port
        listener, err := net.Listen("tcp", "127.0.0.1:2110")
        checkError(err)
        for{
                if conn, err := listener.Accept(); err == nil{
                        //If err is nil then that means that data is available for us so we t
                        go handleProtoClient(conn, c)
                } else{
                        continue
                }
        }
}

func handleProtoClient(conn net.Conn, c chan *TransferOutMismatch.TransferOutMismatch) {

        //Close the connection when the function exits
        defer conn.Close()

        //Create a data buffer of type byte slice with capacity of 4096
        data := make([]byte, 4096)

        //Read the data waiting on the connection and put it in the data buffer
        n,err:= conn.Read(data)
        checkError(err)
        fmt.Println("Decoding Protobuf message")

        //Create an struct pointer of type ProtobufTest.TestMessage struct
        protodata := new(TransferOutMismatch.TransferOutMismatch)

        //Convert all the data retrieved into the ProtobufTest.TestMessage struct type
        err = proto.Unmarshal(data[0:n], protodata)

        checkError(err)
        //Push the protobuf message into a channel
        c <- protodata
}

func checkError(err error){
    if err != nil {
        fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
        os.Exit(1)
    }
}
