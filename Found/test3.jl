using DataFrames
using CSV
using RDatasets
using Dates

file_1 = "F:\\Program\\Julia\\Julia-Quant\\Found\\000539.csv"
df_1 = CSV.read(file_1, header=true)
colnames = ["date", "per_10000_000539", "seven_day_year_000539"]
names!(df_1, Symbol.(colnames))
println(df_1)

file_2 = "F:\\Program\\Julia\\Julia-Quant\\Found\\001134.csv"
df_2 = CSV.read(file_2, header=true)
colnames = ["date", "per_10000_001134", "seven_day_year_001134"]
names!(df_2, Symbol.(colnames))
println(df_2)

file_3 = "F:\\Program\\Julia\\Julia-Quant\\Found\\070008.csv"
df_3 = CSV.read(file_3, header=true)
colnames = ["date", "per_10000_070008", "seven_day_year_070008"]
names!(df_3, Symbol.(colnames))
println(df_3)

a = join(join(df_1, df_2, on=[:date], kind=:inner), df_3, on=[:date], kind=:inner)
# 为方便筛选，转化为时间类型
a[:date] = Date.(a[:date], DateFormat("yyyy/m/d"))
println(showcols(a))
a = a[(a[:date].>Date.(2018,6,1)), :]

# b = mean(a[[:date, :per_10000_000539, :per_10000_001134, :per_10000_070008]])
b = sum(a[:per_10000_000539])/size(a)[1]
c = sum(a[:per_10000_001134])/size(a)[1]
d = sum(a[:per_10000_070008])/size(a)[1]
# println("2018.6.1——2018.8.31中银余额理财收益率概览")
println("000539, 中银活期宝：", Float16(b))
println("001134, 广发天天利货币E类：", Float16(c))
println("070008, 嘉实货币：", Float16(d))
