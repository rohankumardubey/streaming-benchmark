package org.viirya.spark.streaming.benchmark

object DataSchema {

  // Catalyst DDL datatype of test data.
  val auth_events_dt = "struct<city:string,firstName:string,gender:string,itemInSession:bigint,lastName:string,lat:double,level:string,lon:double,registration:bigint,sessionId:bigint,state:string,success:boolean,ts:bigint,userAgent:string,userId:bigint,zip:string>"
  val listen_events_dt = "struct<artist:string,auth:string,city:string,duration:double,firstName:string,gender:string,itemInSession:bigint,lastName:string,lat:double,level:string,lon:double,registration:bigint,sessionId:bigint,song:string,state:string,ts:bigint,userAgent:string,userId:bigint,zip:string>"
  val page_view_events_dt = "struct<artist:string,auth:string,city:string,duration:double,firstName:string,gender:string,itemInSession:bigint,lastName:string,lat:double,level:string,lon:double,method:string,page:string,registration:bigint,sessionId:bigint,song:string,state:string,status:bigint,ts:bigint,userAgent:string,userId:bigint,zip:string>"
  val status_change_events_dt = "struct<auth:string,city:string,firstName:string,gender:string,itemInSession:bigint,lastName:string,lat:double,level:string,lon:double,registration:bigint,sessionId:bigint,state:string,ts:bigint,userAgent:string,userId:bigint,zip:string>"
}
