SSDB Go API Documentation {#mainpage}
============

@author: [matishsiao]

## About

All SSDB operations go with ```ssdb.Client.Do()```, it accepts variable arguments. The first argument of Do() is the SSDB command, for example "get", "set", etc. The rest arguments(maybe none) are the arguments of that command.

The Do() method will return an array of string if no error. The first element in that array is the response code, ```"ok"``` means the following elements in that array(maybe none) are valid results. The response code may be ```"not_found"``` if you are calling "get" on an non-exist key.

Refer to the [PHP documentation](http://www.ideawu.com/ssdb/docs/php/) to checkout a complete list of all avilable commands and corresponding responses.

## gossdb is not thread-safe(goroutine-safe)

Never use one connection(returned by ssdb.Connect()) through multi goroutines, because the connection is not thread-safe.

## gossdb has support connection pool Automatic reuse
## support functions
	key/value functions
	hash functions
## Example

	package main
	
	import (
			"fmt"
			"os"
			"github.com/matishsiao/gossdb/ssdb"
		   )
		   
	func main(){
		ip := "127.0.0.1";
		port := 8888;
		db, err := ssdb.Connect(ip, port);
		if(err != nil){
		        os.Exit(1);
		}
		
		var val interface{};
		db.Set("a", "xxx")
		val, err = db.Get("a");
		fmt.Printf("get A:%s\n", val)
		val, err = db.Get("a");
		fmt.Printf("Get val %s\n", val);
		db.Set("count", "1")
		fmt.Printf("Set Count:%d\n",1)
		val, err = db.Incr("count", 5)
		fmt.Printf("Get Count:%s\n",val) 
		db.SetX( "expireT", "Test", 5)
		
		val,err = db.Exists("expireT")
		fmt.Printf("Exists expireT:%v\n",val)
		val,err = db.Expire("expireT",5) 
		fmt.Printf("Expire expireT:%v\n",val,err) 
		val,err = db.KeyTTL("expireT") 
		fmt.Printf("KeyTTL expireT:%v\n",val,err) 
		val, err = db.SetNew("b","TestB" )
		fmt.Printf("SetNew B:%v\n",val) 
		val, err = db.GetSet("a","new_value")
		fmt.Printf("GetSet A status:%v\n", val)
		val, err = db.Get("a")
		fmt.Printf("Get val A %s\n", val)
		val, err = db.Get("expireT")
		fmt.Printf("Get expireT:%s\n",val) 
		val, err = db.Scan("","",10)  
		for k,v := range val.(map[string]interface{}) { 
			fmt.Printf("Scan[%s]%s\n",k,v)
		}
		fmt.Printf("HashGetAll:%v\n",val)
		val, err = db.HashSet("mdz-2014","test","10")
		val, err = db.HashSet("mdz-2014","1231-0800","5")
		val, err = db.HashSet("mdz-2014","1231-0900","1")
		val, err = db.HashSet("mdz-2014","1231-1000","10")
		val, err = db.HashSet("mdz-2015","1231-1100","5")
		val, err = db.HashSet("mdz-2015","1231-1200","1")
		val, err = db.HashGet("mdz-2014","test")
		fmt.Printf("HashGet:%s\n",val)  
		val, err = db.HashIncr("mdz-2014","test",5) 
		fmt.Printf("HashIncr:%s\n",val)
		val, err = db.HashExists("hash","test")
		fmt.Printf("HashExists:%v\n",val)
		val, err = db.HashSize("mdz-2014")
		fmt.Printf("HashSize:%d\n",val)
		val, err = db.HashScan("mdz-2014","1230","1231-2",10)
		for k,v := range val.(map[string]interface{}) { 
			fmt.Printf("HashScan[%s]%s\n",k,v)
		}
		fmt.Printf("HashScan:%v\n",val)
		multiSet := make(map[string]interface{})
		multiSet["A"] = 1
		multiSet["B"] = 2
		multiSet["C"] = 3
		val, err = db.HashMultiSet("mdz-2014",multiSet)
		fmt.Printf("HashMultiSet:%v\n",val)
		val, err = db.HashMultiGet("mdz-2014",[]string{"A","B"})  
		for k,v := range val.(map[string]interface{}) { 
			fmt.Printf("HashMultiGet[%s]%s\n",k,v)
		}
		fmt.Printf("HashMultiGet:%v\n",val)
		if ssdb.SSDBM != nil {
			ssdb.SSDBM.Info()
		} 
		return
	}

