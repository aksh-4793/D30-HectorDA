package utils

import(
	"fmt"
	"github.com/BurntSushi/toml"
	"custom/model"
	"os"
	"github.com/gocql/gocql"
	"time"
	"encoding/json"
	"custom/proto_types/TransferOutMismatch"
)
var CassandraConf model.CassandraConfig
var CassandraChan chan *gocql.Session
var CassandraSession *gocql.Session
func init() {


	_,err := toml.DecodeFile("/root/workspace/D30-HectorDA/conf/cassandra.toml",&CassandraConf)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	
	CassandraChan = make(chan *gocql.Session,50)
}

func getSession() (*gocql.Session,error) {

	select {

		case CassandraSession := <-CassandraChan:
			return 	CassandraSession,nil
		case <-time.After(100 * time.Millisecond):
			cluster := gocql.NewCluster(CassandraConf.Host)
 			cluster.Keyspace = "all_trade"
			cluster.ProtoVersion = 3
			session, err := cluster.CreateSession()
					
			if err != nil {
				panic(err)
			}

			return session,nil
	}
}


func queueSession(session *gocql.Session) {

	select {
		case CassandraChan <- session:
			// session enqueued
		default:
			fmt.Println("Blocked")
			session.Close()
	}

}

func TestQuery(query string) (string){
	
	session,_ := getSession()
 	iter := session.Query(query).Iter()
	result, _ := iter.SliceMap()
	fmt.Println(result)

	jsonString, err := json.Marshal(result)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}	
	go queueSession(session)	
	return string(jsonString)
}

func Execute(data *TransferOutMismatch.TransferOutMismatch) {
	session,_ := getSession()
	record := *data

	fmt.Println("Inserting...")

	if err := session.Query(`INSERT INTO tranferoutmismatch ( transfer_out_mismatch_pk,transactionId,transactionType,requestNo,company,dataErrorDateTime,createBy,createDateTime,updateBy,updateDateTime )	VALUES ( ?,?,?,?,?,?,?,?,?,? )`,gocql.TimeUUID(), record.GetTransactionId(), record.GetTransactionType() ,record.GetRequestNo(), record.GetCompany(), record.GetDataErrorDateTime(), record.GetCreateBy(), record.GetCreateDateTime(), record.GetUpdateBy(), record.GetUpdateDateTime()).Exec(); err != nil {
		fmt.Println(err.Error())	
	}
}
