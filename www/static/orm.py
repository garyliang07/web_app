#建立类与数据库表的映射，对数据库进行封装
import asyncio, logging
import aiomysql

def log(sql, args=()):#记录程序信息
	logging.info('SQL: %s' % sql)

#创建连接池
@asyncio.coroutine
def create_pool(loop, **kw):
	logging.info('create database connection pool...')
	global __pool  #全局变量控制连接池，方便其他函数调用
	__pool = yield from aiomysql.create_pool(
		#(Dictionary) get() 函数返回指定键的值，如果值不在字典中返回默认值。
		host=kw.get('host', 'localhost'),
		port=kw.get('port', 3306),
		user=kw['user'],#字典的取值
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset', 'utf-8'),
		autocommit=kw.get('autocommit', True),
		maxsize=kw.get('maxsize', 10),
		minsize=kw.get('minsize', 1),
		loop=loop
	)

#封装执行sql的代码，调用时只需传入sql所需参数即可
@asyncio.coroutine
def select(sql, args, size=None):
	log(sql, args)
	global __pool#主函数中不用global声明变量，仍然可以修改全局变量。而在普通函数中，需要global声明变量，才可以修改全局变量。
	with (yield from __pool) as conn: #从连接池取一个连接
		cur = yield from conn.cursor(aiomysql.DictCursor)
		yield from cur.execute(sql.replace('?', '%s'), args or ())#把sql里的字符串占位符？换成python中的%s
		if size:
			rs = yield from cur.fetchmany(size)#只读取size条记录
		else:
			rs = yield from cur.fetchall()
		yield from cur.close()
		logging.info('row returned: %s' % len(rs))#返回的rs是一个list，每个元素是一个dict，有多少个dict就有多少行记录
		return rs

#实现sql语句：insert,update,ddelete
@asyncio.coroutine
def execute(sql, args):
	log(sql)
	with (yield from __pool) as conn:
		if not autocommit:
			await conn.begin() #协程开始启动
		try:
			cur = yield from conn.cursor()
			yield from cur.execute(sql.replace('?', '%s'), args)
			affected = cur.rowcount#获得受影响的行数
			yield from cur.close()
			if not autocommit:
				await conn.commit() #提交事务
		except BaseException as e:
			if not autocommit:
				await conn.rollback() #回滚当前启动的协程
			raise
		return affected

#按参数个数制作占位符字符串，用于生成SQL语句
def create_args_string(num):
	L = []
	for n in range(num): #SQL的占位符是？，num是多少就插入多少个占位符
		L.append('?')
	return ', '.join(L) #将L拼接成字符串返回，例如num=3时："?, ?, ?"。同时，str.join(元组、列表、字典、字符串) 之后生成的只能是字符串。所以很多地方很多时候生成了元组、列表、字典后，可以用 join() 来转化为字符串。
	
# ORM
class Field(object):#数据类型的基类

	def __init__(self, name, column_type, primary_key, default):
		self.name = name#字段名
		self.column_type = column_type#字段类型
		self.primary_key = primary_key#主键
		self.default = default#默认值

	def __str__(self):#print(Field_object)时，返回类名Field，数据类型，列名
		return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
#默认可变字符串，长度100字节
#和char相对应，char是固定长度，字符串长度不够会自动补齐，varchar则是多长就是多长，但最长不能超过规定长度
	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):#定义可传入参数
		super().__init__(name, ddl, primary_key, default)#对应列名、数据类型、主键、默认值

class BooleanField(Field):

	def __init__(self, name=None, default=None):#定义可传入参数
		super().__init__(name, 'boolean', False, default)#对应列名、数据类型、主键、默认值

class IntegerField(Field):

	def __init__(self, name=None, primary_key=False, default=0):#定义可传入参数
		super().__init__(name, 'bigint', primary_key, default)#对应列名、数据类型、主键、默认值

class FloatField(Field):

	def __init__(self, name=None, primary_key=False, default=0.0):#定义可传入参数
		super().__init__(name, 'real', primary_key, default)#对应列名、数据类型、主键、默认值

class TextField(Field):

	def __init__(self, name=None, default=None):#定义可传入参数
		super().__init__(name, 'text', False, default)#对应列名、数据类型、主键、默认值

# metaclass是类的模板，所以必须从'type'类型派生
class ModelMetaclass(type):

	def __new__(cls, name, bases, attrs):	#接收的参数：当前准备创建的类的对象；类的名字；类继承的父类集合；类的方法集合。
	    if name=='Model':
	    	return type.__new__(cls, name, bases, attrs)# 排除掉对Model类的修改；
	    #程序能执行到这儿，那就已经排除了当前类是Model这种情况，上面两句代码起的就是这个作用
        #从这儿开始当前类只可能是User类、Blog类、Comment类
	    # 获取table名称：如果User类中没有定义__table__属性，那默认表名就是类名，也就是User
	    tableName = attrs.get('__table__', None) or name
	    logging.info('found model: %s (table: %s)' % (name, tableName))
	    # 获取所有的Field和主键名：
	    mappings = dict() # 存储列名和数据类型
	    fields = [] #存储非主键的列
	    primaryKey = None #用于主键查重
	    #attrs是User类的属性集合，是一个dict，需要通过items函数转换为[(k1,v1),(k2,v2)]这种形式，才能用for k, v in来循环
	    for k, v in attrs.items():
	    	if isinstance(v, Field):
	    		logging.info('  found mappings: %s ==> %s' % (k, v)) #拆开k, v
	    		mappings[k] = v #为何拆开dict后又创建一个dict？因为后面要把attrs的数据全部删除
	    		if v.primary_key: #找到主键
	    			if primaryKey: #存在两个主键的情况，抛出错误
	    				raise RuntimeError('Duplicate primary key for field: %s' % k)
	    			primaryKey = k #为主键赋值
	    		else:
	    			fields.append(k) #非主键的属性名，存到非主键字段名的list中
	    if not primaryKey: #错误：没有找到主键
	    	raise RuntimeError('Primary key not found.')
	    for k in mappings.keys(): #把类中的原有属性删除	
	    	attrs.pop(k)
	    #fields中的值都是字符串，下面这个匿名函数的作用是在字符串两边加上``生成一个新的字符串，为了后面生成sql语句做准备
	    escaped_fields = list(map(lambda f: '`%s`' % f, fields))
	    # ATTRS为类的方法
	    attrs['__mappings__'] = mappings # 保存属性和列名的映射关系，把mappings这个dict存入attrs这个dict中
	    attrs['__table__'] = tableName
	    attrs['__primary_key__'] = primaryKey # 主键属性名
	    attrs['__fields__'] = fields # 除主键外的属性名
	    # 构造默认的SELECT, INSERT, UPDATE和DELETE语句，下面就是四个sql语句，然后分别存入attrs
	    attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ','.join(escaped_fields), tableName)
	    attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
	    attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ','.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
	    attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
	    return type.__new__(cls, name, bases, attrs)
#简述元类所做的事情：
#首先获取所有的Field, 主键名。
#把属性名，数据存入mappings这个dict中
#最后，把所需的信息存入attrs这个dict中
#以上仅包含了属性，但方法则在Model类中

class Model(dict, metaclass=ModelMetaclass):
	# 定义一个对应数据库数据类型的模板类。通过继承，获得dict的特性和元类的类与数据库的映射关系
	# 由模板类衍生其他类时，这个模板类没重新定义__new__()方法，因此会使用父类ModelMetaclass的__new__()来生成衍生类，从而实现ORM
	def __init__(self, **kw):
		super(Model, self).__init__(**kw) 
		#调用父类的方法，确保父类被正确的初始化了，仅调用一次；
		#没有这个方法，获取dict的值需要通过d[k]的方式，有这个方法就可以通过属性来获取值，也就是d.k
		#getattr、settattr实现属性动态绑定和获取
	def __getattr(self, key):     #封装内部获取函数 
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)

	def __setattr(self, key, value):
		self[key] = value   #如果没有则返回None	
 	#上面两个方法是用来获取和设置**kw转换而来的dict的值，而下面的getattr是用来获取当前实例的属性值，不要搞混了
	def getvalue(self, key):
		return getattr(self, key, None)  #如果没有则返回None
	#如果当前实例没有与key对应的属性值时，就需要调用下面的方法了
	def getvalueOrDefault(self, key):
		value = getattr(self, key, None)
		if value is None:
			field = self.__mappings__[key]   #查取属性对应的列的数量类型默认值
			if field.default is not None:#如果查询出来的字段具有default属性，那就检查default属性值是方法还是具体的值
                #如果是方法就直接返回调用后的值，如果是具体的值那就返回这个值
				value = field.default() if callable(field, default) else field.default
				logging.debug('using default value for %s: %s' % (key, str(value)))
				setattr(self, key, value)   #查到key对应的value后就设置为当前实例的属性
			return value

	@classmethod #这个装饰器是类方法的意思，这样就可以不创建实例直接调用类的方法
	async def findAll(cls, where=None, args=None, **kw):
		' find objects by where clause. ' #一个对象代表数据库表中的一行，通过条件查询对象
		sql = [cls.__select__]
		if where: #若有where条件就在sql语句插入字符串和值
			sql.append('where')
			sql.append(where)
		if args is None:
			args = []
		orderBy = kw.get('orderBy', None)
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit = kw.get('limit', None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit, int):   #若limit为整数，将结果的前n个返回
				sql.append('?')
				args.append(limit)
			elif isinstance(limit, tuple) and len(limit) == 2:
				#若limit为tuple，则前一个值为索引，后一个为结果数目
				sql.append('?, ?')
				args.extend(limit) #用extend()把tuple的小括号去掉
			else:
				raise ValueError('Invalid limit value: %s' % str(limit))
		rs = await select(' '.join(sql), args) #让select函数执行
		return [cls(**r) for r in rs] #返回一个列表。每个元素都是一个dict，相当于一行记录

	@classmethod
	async def findNumber(cls, selectField, where=None, args=None):
		' find number by select and where. ' #根据where查询结果的数量
		sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)] # _num_是SQL的一个字段别名用法，AS关键字可以省略
		if where:
			sql.append('where')
			sql.append(where)
		rs = await select(' '.join(sql), args, 1)
		if len(rs) == 0:
			return None
		return rs[0]['_num_']#rs是个list，而这个list的第一项对应的是个dict，这个dict中的_num_属性值就是结果数

	@classmethod
	@asyncio.coroutine
	def find(cls, pk):
		' find object by primary key. '
		rs = yield from select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
		if len(rs) == 0:
			return None
		return cle(**rs[0])
#save、update、remove这三个方法需要管理员权限才能操作，所以不定义为类方法，需要创建实例之后才能调用
	@asyncio.coroutine
	def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))#把实例的非关键字属性值全都查出来然后存入args这个list
		args.append(self.getValueOrDefault(self.__primary_key__)))#把主键找出来加到args这个list的最后
		rows = yield from execute(self.__insert__, args)#执行sql语句后返回影响的结果行数
		if rows != 1: #一个实例只能插入一行数据，所以返回的影响行数一定为1,如果不为1那就肯定错了
			logging.warn('failed to insert record: affected rows:%s' % rows)

	async def update(self):
		args = list(map(self.getValue, self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows = await execute(self.__update__, args)
		if rows != 1:
			logging.warn('failed to update by primary key: affected rows: %s' % rows)

	async def remove(self):
		args = [self.getValue(self.__primary_key__)]
		rows = await execute(self.__delete__, args)
		if rows != 1:
			logging.warn('failed to remove by primary key: affected rows: %s' % rows)