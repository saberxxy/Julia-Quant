using DataFrames
using CSV
using Dates
using MySQL


function nasdaq()
    file_1 = "F:\\Quant\\纳斯达克-上证指数分析\\^IXIC.csv"
    df_1 = CSV.read(file_1, header=true)
    # 修改数据类型
    df_1[:Date] = Date.(df_1[:Date], Dates.DateFormat("yyyy/m/d"))
    # 条件筛选 & 筛选列
    df_1 = df_1[(df_1[:Date].>Date.(2015,12,31)) .&
        (df_1[:Date].<Date.(2018,8,1)), [:Date, :Open, :Close]]
    # 重命名
    colnames = ["NAS_DATE", "NAS_OPEN", "NAS_CLOSE"]
    names!(df_1, Symbol.(colnames))
    return df_1
end

# println(DataFrames.showcols(df_1))
function sse()
    file_2 = "F:\\Quant\\纳斯达克-上证指数分析\\^SSE.csv"
    df_2 = CSV.read(file_2, header=true)
    # 筛选列
    df_2 = df_2[:, [:SDATE, :OPEN, :CLOSE]]
    # 修改数据类型
    df_2[:SDATE] = Date.(df_2[:SDATE], Dates.DateFormat("yyyy/m/d"))
    df_2 = df_2[(df_2[:SDATE].>Date.(2015,12,31)) .&
        (df_2[:SDATE].<Date.(2018,8,1)), :]
    # 重命名
    colnames = ["SZ_DATE", "SZ_OPEN", "SZ_CLOSE"]
    names!(df_2, Symbol.(colnames))
    return df_2
end


ixic_result = nasdaq()
sse_result = sse()

# println(ixic_result)
# println(sse_result)


# 连接MySQL数据库
# conn = MySQL.connect("localhost", "root", "123456", db = "test")
# try
#     MySQL.query(conn, "truncate table test.nas")
#     # 依旧是解决中文乱码的问题，原因主要是转码时候的各种坑，操作系统底层编码结构的坑
#     # 以下是R的解决方案，Julia类似，只不过编码改成utf8
#     # dbSendQuery(conn_2, 'SET NAMES gbk')
#     MySQL.query(conn, "SET NAMES utf8")
#     # 语言的编码，数据库的编码，语言所运行操作系统的编码，数据库运行操作系统的编码，
#     # 中间件的编码，容器的编码等等，都会有坑，一旦遇到编码问题不要方，多试几下基本就好了
# catch
#     pass
# end
# my_stmt = MySQL.Stmt(conn, """insert into test.nas(`nas_date`, `nas_open`, `nas_close`)
#                             values(?, ?, ?);""")
# size()，计算DataFrame的维度，等同于R中的dim()
# dataframe切片，
# for i = 1:size(ixic_result)[1]
# # for i = 1:5
#     a = ixic_result[i:i, [:NAS_DATE]][1][1]
#     b = ixic_result[i:i, [:NAS_OPEN]][1][1]
#     c = ixic_result[i:i, [:NAS_CLOSE]][1][1]
#     println(i, "    OK")
#     MySQL.execute!(my_stmt, [a, b, c])
# end
# println()
# MySQL.disconnect(conn)

# string() 连接字符串函数
# a = string("1", "2")
# println(a)

# 将纳斯达克数据插入数据库
function insert_nas()
    conn = MySQL.connect("localhost", "root", "123456", db = "test")

    my_stmt = MySQL.Stmt(conn, """insert into test.nas(`nas_date`, `nas_open`, `nas_close`)
                                values(?, ?, ?);""")
    for i = 1:size(ixic_result)[1]
        a = ixic_result[i:i, [:NAS_DATE]][1][1]
        b = ixic_result[i:i, [:NAS_OPEN]][1][1]
        c = ixic_result[i:i, [:NAS_CLOSE]][1][1]
        # println(i, "    OK")
        MySQL.execute!(my_stmt, [a, b, c])
    end
    println("NAS OK")
    MySQL.disconnect(conn)
end

function insert_sse()
    conn = MySQL.connect("localhost", "root", "123456", db = "test")

    my_stmt = MySQL.Stmt(conn, """insert into test.sse(`sz_date`, `sz_open`, `sz_close`)
                                values(?, ?, ?);""")
    for i = 1:size(sse_result)[1]
        a = sse_result[i:i, [:SZ_DATE]][1][1]
        b = sse_result[i:i, [:SZ_OPEN]][1][1]
        c = sse_result[i:i, [:SZ_CLOSE]][1][1]
        # println(i, "    OK")
        MySQL.execute!(my_stmt, [a, b, c])
    end
    println("SSE OK")
    MySQL.disconnect(conn)
end

insert_nas()
insert_sse()
