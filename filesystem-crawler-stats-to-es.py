from elasticsearch import Elasticsearch,helpers
import os,sys,time,pwd
from datetime import datetime
import hashlib

#argument1 = int(sys.argv[1])

actions = []
types = "_doc"
esIndexName = ""
top = ""

es = Elasticsearch(hosts=['elastic-test.winstorm.nssl:9200'], timeout=30,http_auth=('user.name', 'password'))

def main():
	global esIndexName
	global top
	top = input("Enter path to filesystem to crawl: ")
	esIndexName = input("Index name to send files: ")
	response = es.indices.create(index=esIndexName,body={"settings":{"number_of_shards":3,"number_of_replicas":0}},ignore=400)
	#response = es.indices.create(index=esIndexName,ignore=400)
	print(response)
	data = viewTree(top)
	traverseTree(data)

def viewTree(top):
	data = os.walk(top)
	return data

def traverseTree(data):
	for dirPath,dirNames,fileNames in data:
		#directory level assessments
		global actions
		actions = []
		for fileName in fileNames:
			#file level assessements
			filePath = os.path.join(dirPath,fileName)
			#print(filePath)
			try:
				esID = createID(filePath)
			except UnicodeEncodeError as error:
				continue	
			fileMetadata = statFile(filePath)
			#print(fileMetadata)
			prepareMetadata(filePath,dirPath,fileName,fileMetadata,esID,top)

		#for dirName in dirNames:
		for fileName in dirNames:
			#directory level assessment
			#filePath = os.path.join(dirPath,dirName)
			filePath = os.path.join(dirPath,fileName)
			#print(filePath)
			try:
				esID = createID(filePath)
			except UnicodeEncodeError as error:
				continue	
			fileMetadata = statFile(filePath)
			#print(fileMetadata)		
			prepareMetadata(filePath,dirPath,fileName,fileMetadata,esID,top)


		if len(actions)>0:
			if len(actions)>500:
				floorQuotient = len(actions)//500
				remaining = len(actions)%500
				counter = 500
				for v in range(floorQuotient):
					helpers.bulk(es,actions=actions[v*500:counter])
					#print(len(actions[v*500:counter]))
					#time.sleep(1)
					counter = counter+500
				helpers.bulk(es,actions=actions[-remaining:])
				#print(len(actions[-remaining:]))
				#time.sleep(1)

			else:
				helpers.bulk(es,actions=actions)
				#print(len(actions))
				#time.sleep(.5)
				#pass

def prepareMetadata(filePath,dirPath,fileName,fileMetadata,esID,top):
	if fileMetadata!=0:
		global actions
		global esIndexName
		payload = {
				"_index":esIndexName,
				"_id":esID,
				"_type":types,
				"directory":dirPath,
				"file":fileName,
				"absolutePath":filePath,
				"uid":fileMetadata[0],
				"user":getAccount(fileMetadata[0]),
				"gid":fileMetadata[1],
				"size":fileMetadata[2],
				"atime":fileMetadata[3],
				"mtime":fileMetadata[4],
				"ctime":fileMetadata[5],
				"perm":fileMetadata[7],
				"statTimestamp":fileMetadata[6],
				"filesystem":top
				} 
		actions.append(payload)
	else:
		pass
		
def statFile(filePath):
	try:
		statResult = os.stat(filePath)
		fileMetadata = [
						statResult.st_uid,
						statResult.st_gid,
						statResult.st_size,
						formatTimestamp(statResult.st_atime),
						formatTimestamp(statResult.st_mtime),
						formatTimestamp(statResult.st_ctime),
						formatTimestamp(time.time()),
						oct(statResult.st_mode)[-3:]
						]
		return fileMetadata
	except IOError as error:
		handleExceptions(esIndexName,error,filePath)
		fileMetadata = 0
		return fileMetadata
	except OSError as error:
		handleExceptions(esIndexName,error,filePath)
		fileMetadata = 0
		return fileMetadata
	except PermissionError as error:
		handleExceptions(esIndexName,error,filePath)
		fileMetadata = 0
		return fileMetadata
	except TypeError as error:
		handleExceptions(esIndexName,error,filePath)
		fileMetadata = 0
		return fileMetadata
	except UnicodeEncodeError as error:
		#handleExceptions(esIndexName,error,filePath)
		fileMetadata = 0
		return fileMetadata
	except Exception as error:
		handleExceptions(esIndexName,error,filePath)
		fileMetadata = 0
		return fileMetadata

def handleExceptions(esIndexName,error,filePath):
	print(esIndexName,error,filePath)
	f = open('errors_'+esIndexName+'.txt','a')
	f.write(str(error)+'|'+filePath+'\n')
	f.close()

def formatTimestamp(nanoseconds):
	ts = datetime.fromtimestamp(nanoseconds).strftime("%Y/%m/%d %H:%M:%S")
	return ts

def createID(absolutePath):
	digest = hashlib.sha1(absolutePath.encode()).hexdigest()
	return digest

def getAccount(uid):
	try:
		return pwd.getpwuid(uid)[0]
	except KeyError:
		return 'orphaned uid'

def verifyTaskSelection(taskNumber):
	if taskNumber not in tasks.keys():
		print("The value you typed is not in the task list")
	else:
		if taskNumber in list(tasks.keys())[1:]:
			return taskNumber
		else:
			print('Goodbye')
			quit()	

def mainMenu():
	print("Welcome. What would you like to examine today?")
	populateTaskMenu()
	taskNumber = int(input("Type the number associated with the task: "))
	os.system('clear')
	taskNumber = verifyTaskSelection(taskNumber)
	return taskNumber

def populateTaskMenu():
	print()
	for k,v in tasks.items():
		print("- "+str(k)+":",v)
		print()

if __name__ == '__main__':
	main()
