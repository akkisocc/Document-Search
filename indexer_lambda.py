import json
import boto3
import time
import os
from urllib.parse import unquote_plus
from elasticsearch import Elasticsearch, RequestsHttpConnection

"""
Returns status of document being processed
"""
def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

"""
Returns pages of the scanned document processed by Textract
"""
def getJobResults(jobId):

    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)

    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)
        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

"""
Indexes the document to Elasticsearch
"""
def addToESIndex(s3URI, objectName, text):
    lambdaSession = boto3.Session()
    region = lambdaSession.region_name
    # Replace below line with your elasticsearch host
    host= ""
    es = Elasticsearch(hosts = [{'host': host, 'port':443}],use_ssl = True,verify_certs = True,connection_class = RequestsHttpConnection)
    document = {"name": "{}".format(objectName),"s3URI": "{}".format(s3URI),"content": text }
    es.index(index="textractsearch", doc_type="document", id=objectName, body=document)
    print("indexing done")



"""
Revoked when the objects are uploaded to S3 bucket
"""
def lambda_handler(event, context):


    client = boto3.client('textract')
    s3client = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    document = unquote_plus(event['Records'][0]['s3']['object']['key'])
    documenturl = '{}/{}/{}'.format(s3client.meta.endpoint_url, bucket, document)

    #process using S3 object
    response = client.start_document_text_detection(
        DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document}})

    #Get the text blocks
    jobId=response["JobId"]
    if(isJobComplete(jobId)):
        response = getJobResults(jobId)
    text=""
    for resultPage in response:
        text=""
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                text += " "+item["Text"]+" "

        #print(text)
        addToESIndex(documenturl,document, text)
