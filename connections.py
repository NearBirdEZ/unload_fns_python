import psycopg2
import paramiko
import requests


class Connections:

    def __init__(self, file):
        """На вход передается файл с наименованием конфига с доступами"""
        server = self.__take_properties(file, 'server')
        elastic = self.__take_properties(file, 'elastic')
        db = self.__take_properties(file, 'db')

        self.login_server = server[0]
        self.password_server = server[1]
        self.host_server = server[2]
        self.port_server = server[3]

        self.login_elastic = elastic[0]
        self.password_elastic = elastic[1]
        self.host_elastic = elastic[2]
        self.port_elastic = elastic[3]

        self.login_db = db[0]
        self.password_db = db[1]
        self.host_db = db[2]
        self.port_db = db[3]
        self.database_db = db[4]

    def __take_properties(self, file, type_auth):
        # Обнуление переменных в случае, если данных нет в файле конфиге
        user, host, port, password, database = (0, 0, 0, 0, 0)
        with open(file, 'r') as prop:
            for line in prop:
                if line.strip().startswith(f'user_{type_auth}'):
                    user = line.strip().split('=')[1]
                elif line.strip().startswith(f'host_{type_auth}'):
                    host = line.strip().split('=')[1]
                elif line.strip().startswith(f'port_{type_auth}'):
                    port = int(line.strip().split('=')[1])
                elif line.strip().startswith(f'password_{type_auth}'):
                    password = line.strip().split('=')[1]
                elif line.strip().startswith('database'):
                    database = line.split('=')[1].strip()
        return user, password, host, port, database

    def to_ssh(self, cmd):
        """На вход передаются команды для сервера, на выход отдается результат"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=self.host_server,
                       username=self.login_server,
                       port=self.port_server,
                       password=self.password_server)
        stdin, stdout, stderr = client.exec_command(cmd)
        data, error = stdout.read().decode('utf-8').strip().split('\n'), \
                      stderr.read().decode('utf-8').strip().split('\n')
        client.close()
        return data, error

    def to_elastic(self, data, index='*'):
        """
        На вход принимает запрос для поиска, возвращает json

        Примеры запросов

        {"size" : 1 }

        { "query" : { "bool" : { "must" :
        [{ "term" : {"requestmessage.fiscalDriveNumber.raw" : "9999999999"} },
        {"term" : {"requestmessage.kktRegId.raw" : "7777777777"}},
        {"term" : {"requestmessage.fiscalDocumentNumber" : "888888888"}}] } } }
        """

        headers = {
            'Content-Type': 'application/json',
        }
        params = (
            ('pretty', ''),
        )

        response = requests.post(f'http://{self.host_elastic}:{self.port_elastic}/{index}/_search',
                                 headers=headers, params=params, data=data,
                                 auth=(self.login_elastic, self.password_elastic))
        return response.json()

    def to_sql(self, request):
        """
        На вход подается sql запрос
        На выходе массив построчно.
        :param request:
        :return:
        """
        connect = psycopg2.connect(
                            database=self.database_db,
                            user=self.login_db,
                            password=self.password_db,
                            host=self.host_db,
                            port=self.port_db
                            )
        cursor = connect.cursor()
        cursor.execute(request)
        rows = cursor.fetchall()
        return rows


def test():
    connect = Connections('properties')
    print(connect.to_ssh('cd /var/log/prom/ && ls'))
    print()
    print(connect.to_elastic('{"size" : 1}'))
    print()
    print(connect.to_sql('select * from kkt limit 1'))


if __name__ == '__main__':
    test()
