from connections import Connections

connect = Connections('properties')

data = '{"size" : 1}'

print(connect.to_elastic(data, 'bso_corre*'))
