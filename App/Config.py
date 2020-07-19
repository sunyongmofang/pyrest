import toml


class Config(object):
    config = None

    def __init__(self, configPath: str):
        with open(configPath, 'r') as fp:
            toml_string = fp.read()
            self.tomlObj = toml.loads(toml_string)
        if not Config.config:
            Config.config = ConfigObj(self.tomlObj)


class ConfigObj(object):

    def __init__(self, tomlObj: dict):
        self.DEBUG = 'debug'
        def getValue(obj, key, default): return obj[key] if obj and key in obj else default

        self.http = HttpConf(tomlObj)
        self.jwt = JwtObj(tomlObj)
        self.database = DatabaseObj(tomlObj)
        self.queries = QueriesObj(tomlObj)
        self.debug = getValue(tomlObj, self.DEBUG, False)


class HttpConf(object):

    def __init__(self, tomlObj: dict):
        def getValue(obj, key, default): return obj[key] if obj and key in obj else default

        self.HTTP = 'http'
        self.PORT = 'port'
        selfObj = getValue(tomlObj, self.HTTP, None)

        self.port = getValue(selfObj, self.PORT, 6000)


class JwtObj(object):

    def __init__(self, tomlObj: dict):
        def getValue(obj, key, default): return obj[key] if obj and key in obj else default

        self.JWT = 'jwt'
        self.DEFAULT = 'default'
        self.KEY = 'key'
        self.ALGO = 'algo'
        selfObj = getValue(tomlObj, self.JWT, None)

        self.default = getValue(selfObj, self.DEFAULT, None)
        self.key = getValue(selfObj, self.KEY, None)
        self.algo = getValue(selfObj, self.ALGO, None)


class DatabaseObj(object):

    def __init__(self, tomlObj: dict):
        def getValue(obj, key, default): return obj[key] if obj and key in obj else default
        databaseList = ['postgres']
        databaseType = [x for x in databaseList if x in tomlObj][0]
        selfObj = tomlObj[databaseType]

        self.HOST = 'host'
        self.USER = 'user'
        self.PASS = 'pass'
        self.PORT = 'port'
        self.DATABASE = 'database'
        self.URL = 'URL'

        self.host = getValue(selfObj, self.HOST, 'localhost')
        self.user = getValue(selfObj, self.USER, 'postgres')
        self._pass = getValue(selfObj, self.PASS, 'postgres')
        self.port = getValue(selfObj, self.PORT, 5432)
        self.database = getValue(selfObj, self.DATABASE, 'test')

        self._url = '{databaseType}://{user}:{_pass}@{host}/{database}'.format(
            databaseType=databaseType,
            user=self.user,
            _pass=self._pass,
            host=self.host,
            database=self.database
        )
        self.URL = getValue(selfObj, self.URL, self._url)


class QueriesObj(object):

    def __init__(self, tomlObj: dict):
        def getValue(obj, key, default): return obj[key] if obj and key in obj else default

        self.QUERIES = 'queries'
        self.LOCATION = 'location'
        selfObj = getValue(tomlObj, self.QUERIES, None)

        self.location = getValue(selfObj, self.LOCATION, None)
