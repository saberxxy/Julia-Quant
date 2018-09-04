using DataFrames
using CSV
using Dates
using MySQL

println("aaaaa")
conn = MySQL.connect("localhost", "root", "123456", db = "test")
println(conn)
MySQL.disconnect(conn)