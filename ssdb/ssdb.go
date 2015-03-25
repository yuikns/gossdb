package ssdb

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"sync"
	"io"
	"time"
)

type Client struct {
	sock *net.TCPConn
	recv_buf bytes.Buffer
	reuse	bool
	id int64
	close_flag bool
	last_time int64
	success int64
	count int64
	mu	*sync.Mutex
}

type SSDB struct {
	mu *sync.Mutex
	connect_pool []*Client
	max_connect int
	ip string
	port int
	timeout int
}
var SSDBM *SSDB
const layout = "2006-01-06 15:04:05"
func SSDBInit(ip string, port int,max_connect int) *SSDB {
	return &SSDB{max_connect:max_connect,ip:ip,port:port,timeout:30,mu:&sync.Mutex{}}
}

func (db *SSDB) Recycle() {
	go func() {
		for {
			now := time.Now().Unix()
			db.mu.Lock()
			remove_count := 0
			for i := len(db.connect_pool)-1;i >= 0;i-- {
				v := db.connect_pool[i]
				if v.close_flag || now - v.last_time > int64(db.timeout) {
					v.Close()
					remove_count++
					if len(db.connect_pool) > 1 {
						db.connect_pool = append(db.connect_pool[:i], db.connect_pool[i+1:]...)
					} else {
						db.connect_pool = nil
					}	
				}
			}
			db.mu.Unlock()
			fmt.Println("remove_count:",remove_count)
			time.Sleep(10 * time.Second)
		}
	}()
}


func (db *SSDB) Info() {
	var use,nouse int
	var count,success,close_count int64
	for _,v := range SSDBM.connect_pool {
		//fmt.Printf("[%d][status]:%v\n",k,v.reuse)
		if v.reuse {
			nouse++
		} else {
			use++
		}
		
		if v.close_flag {
			close_count++
		}
		count += v.count
		success += v.success
	}
	failed := count - success
	
	now_time:=time.Now().Format(layout)
	fmt.Printf("[%s] SSDBM Info[IP]:%v [Port]:%v [Max]:%v [Pool]:%v [Use]:%v [NoUse]:%v [Close]:%v [Total]:%v [Success]:%v [Failed]:%v\n",now_time,db.ip,db.port,db.max_connect,len(SSDBM.connect_pool),use,nouse,close_count,count,success,failed)
	
}

func Connect(ip string, port int) (*Client, error) {
	if SSDBM == nil {
		SSDBM = SSDBInit(ip,port,100)
		SSDBM.Recycle()
	}
	SSDBM.mu.Lock()
	for i,v := range SSDBM.connect_pool {
		if v.reuse && !v.close_flag {
			v.mu.Lock()
			v.reuse = true
			v.mu.Unlock()
			return v,nil
		} else if v.close_flag {
			SSDBM.connect_pool = append(SSDBM.connect_pool[:i], SSDBM.connect_pool[i+1:]...)
		}
	}
	SSDBM.mu.Unlock()
	client,err := connect(SSDBM.ip,SSDBM.port)
	if err != nil {
		return nil,err
	}
	if client != nil {
		SSDBM.connect_pool = append(SSDBM.connect_pool,client)
		client.id = time.Now().UnixNano()
		return client,nil
	}
	return nil,nil
}

func connect(ip string, port int) (*Client, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return nil, err
	}
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	var c Client
	c.last_time = time.Now().Unix()
	c.sock = sock
	c.mu = &sync.Mutex{}
	c.reuse = true
	c.close_flag = false
	return &c, nil
}

func (c *Client) Do(args ...interface{}) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reuse = false
	err := c.send(args)
	if err != nil {
		return nil, err
	}
	resp, err := c.recv()
	c.reuse = true
	return resp, err
}


func (c *Client) ProcessCmd(cmd string,args []interface{}) (interface{}, error) {
    c.last_time = time.Now().Unix()
    args = append(args,nil)
    // Use copy to move the upper part of the slice out of the way and open a hole.
    copy(args[1:], args[0:])
    // Store the cmd to args
    args[0] = cmd
    c.mu.Lock()
	defer c.mu.Unlock()
	c.reuse = false
	c.count++
	err := c.send(args)
	if err != nil {
		//fmt.Println("processCmd send error:",err)
		if err == io.EOF {
			c.close_flag = true
		}
		c.reuse = false
		return nil, err
	}
	resp, err := c.recv()
	if err != nil {
		//fmt.Println("processCmd error:",err)
		c.reuse = false
		return nil, err
	}
	c.success++
	c.reuse = true
	if len(resp) == 2 && resp[0] == "ok" {
		switch cmd {
			case "set","del":
				return true, nil
			case "expire","setnx","auth","exists","hexists":
				if resp[1] == "1" {
				 return true,nil
				}	
				return false,nil
			case "hsize":
				val,err := strconv.ParseInt(resp[1],10,64)
				return val,err
			default:
				return resp[1], nil
		}
		
	}else if resp[0] == "not_found" {
		return nil, nil
	} else {
		if resp[0] == "ok" {
			//fmt.Println("Process:",args,resp)
			switch cmd {
				case "hgetall","hscan","hrscan","multi_hget","scan","rscan":
					list := make(map[string]interface{})
					length := len(resp[1:])
					data := resp[1:]
					for i := 0; i < length; i += 2 {
						list[data[i]] = data[i+1]
					}
					return list,nil
				default:
				return resp[1:],nil
			}
		}
		
	}
	
	return nil, fmt.Errorf("bad response")
}

func (c *Client) Auth(pwd string) (interface{}, error) {
	params := []interface{}{pwd}
	return c.ProcessCmd("auth",params)
}

func (c *Client) Set(key string, val string) (interface{}, error) {
	params := []interface{}{key,val}
	return c.ProcessCmd("set",params)
}

func (c *Client) Get(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("get",params)
}

func (c *Client) Del(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("del",params)
}

func (c *Client) SetX(key string,val string, ttl int) (interface{}, error) {
	params := []interface{}{key,val,ttl}
	return c.ProcessCmd("setx",params)
}

func (c *Client) Scan(start string,end string,limit int) (interface{}, error) {
	params := []interface{}{start,end,limit}
	return c.ProcessCmd("scan",params)
}

func (c *Client) Expire(key string,ttl int) (interface{}, error) {
	params := []interface{}{key,ttl}
	return c.ProcessCmd("expire",params)
}

func (c *Client) KeyTTL(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("ttl",params)
}

//set new key if key exists then ignore this operation
func (c *Client) SetNew(key string,val string) (interface{}, error) {
	params := []interface{}{key,val}
	return c.ProcessCmd("setnx",params)
}

//
func (c *Client) GetSet(key string,val string) (interface{}, error) {
	params := []interface{}{key,val}
	return c.ProcessCmd("getset",params)
}

//incr num to exist number value
func (c *Client) Incr(key string,val int) (interface{}, error) {
	params := []interface{}{key,val}
	return c.ProcessCmd("incr",params)
}

func (c *Client) Exists(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.ProcessCmd("exists",params)
}

func (c *Client) HashSet(hash string,key string,val string) (interface{}, error) {
	params := []interface{}{hash,key,val}
	return c.ProcessCmd("hset",params)
}

func (c *Client) HashGet(hash string,key string) (interface{}, error) {
	params := []interface{}{hash,key}
	return c.ProcessCmd("hget",params)
}

func (c *Client) HashDel(hash string,key string) (interface{}, error) {
	params := []interface{}{hash,key}
	return c.ProcessCmd("hdel",params)
}

func (c *Client) HashIncr(hash string,key string,val int) (interface{}, error) {
	params := []interface{}{hash,key,val}
	return c.ProcessCmd("hincr",params)
}

func (c *Client) HashExists(hash string,key string) (interface{}, error) {
	params := []interface{}{hash,key}
	return c.ProcessCmd("hexists",params)
}

func (c *Client) HashSize(hash string) (interface{}, error) {
	params := []interface{}{hash}
	return c.ProcessCmd("hsize",params)
}

//search from start to end hashmap name or haskmap key name,except start word
func (c *Client) HashList(start string,end string,limit int) (interface{}, error) {
	params := []interface{}{start,end,limit}
	return c.ProcessCmd("hlist",params)
}

func (c *Client) HashKeys(hash string,start string,end string,limit int) (interface{}, error) {
	params := []interface{}{hash,start,end,limit}
	return c.ProcessCmd("hkeys",params)
}

func (c *Client) HashGetAll(hash string) (map[string]interface{}, error) {
	params := []interface{}{hash}
	val,err := c.ProcessCmd("hgetall",params)
	if err != nil {
		return nil,err
	} else {
		return val.(map[string]interface{}),err
	}
	
	return nil,nil
}

func (c *Client) HashScan(hash string,start string,end string,limit int) (map[string]interface{}, error) {
	params := []interface{}{hash,start,end,limit}
	val,err := c.ProcessCmd("hscan",params)
	if err != nil {
		return nil,err
	} else {
		return val.(map[string]interface{}),err
	}
	
	return nil,nil
}

func (c *Client) HashRScan(hash string,start string,end string,limit int) (map[string]interface{}, error) {
	params := []interface{}{hash,start,end,limit}
	val,err := c.ProcessCmd("hrscan",params)
	if err != nil {
		return nil,err
	} else {
		return val.(map[string]interface{}),err
	}
	return nil,nil
}

func (c *Client) HashMultiSet(hash string,data map[string]interface{}) (interface{}, error) {
	params := []interface{}{hash}
	for k,v := range data {
		params = append(params,k)
		params = append(params,v)
	}
	return c.ProcessCmd("multi_hset",params)
}

func (c *Client) HashMultiGet(hash string,keys []string) (interface{}, error) {
	params := []interface{}{hash}
	for _,v := range keys {
		params = append(params, v)
	}
	return c.ProcessCmd("multi_hget",params)
}

func (c *Client) HashMultiDel(hash string,keys []string) (interface{}, error) {
	params := []interface{}{hash}
	for _,v := range keys {
		params = append(params, v)
	}
	return c.ProcessCmd("multi_hdel",params)
}


func (c *Client) HashClear(hash string) (interface{}, error) {
	params := []interface{}{hash}
	return c.ProcessCmd("hclear",params)
}


func (c *Client) Send(args ...interface{}) error {
	return c.send(args);
}

func (c *Client) send(args []interface{}) error {
	var buf bytes.Buffer
	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []byte:
			s = string(arg)
		case []string:
			for _, s := range arg {
				buf.WriteString(fmt.Sprintf("%d", len(s)))
				buf.WriteByte('\n')
				buf.WriteString(s)
				buf.WriteByte('\n')
			}
			continue
		case int:
			s = fmt.Sprintf("%d", arg)
		case int64:
			s = fmt.Sprintf("%d", arg)
		case float64:
			s = fmt.Sprintf("%f", arg)
		case bool:
			if arg {
				s = "1"
			} else {
				s = "0"
			}
		case nil:
			s = ""
		default:
			return fmt.Errorf("bad arguments")
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	_, err := c.sock.Write(buf.Bytes())
	return err
}

func (c *Client) Recv() ([]string, error) {
	return c.recv();
}

func (c *Client) recv() ([]string, error) {
	var tmp [1]byte
	for {
		resp := c.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
		n, err := c.sock.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		c.recv_buf.Write(tmp[0:n])
	}
}

func (c *Client) parse() []string {
	resp := []string{}
	buf := c.recv_buf.Bytes()
	var idx, offset int
	idx = 0
	offset = 0

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := buf[offset : offset+idx]
		offset += idx + 1
		//fmt.Printf("> [%s]\n", p);
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				c.recv_buf.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= c.recv_buf.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	//fmt.Printf("buf.size: %d packet not ready...\n", len(buf))
	return []string{}
}

// Close The Client Connection
func (c *Client) Close() error {
	return c.sock.Close()
}
