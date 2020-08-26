from aws_cdk import (
    core,
    aws_dynamodb as ddb,
    aws_s3 as s3,
    aws_dms as _dms,
    aws_iam as _iam,
    aws_glue as _glue,
    aws_s3_assets as S3Assets,
    aws_sns as _sns,
    aws_events as _events,
    aws_events_targets as _events_targets,
    aws_sns_subscriptions as _subscrption,
    aws_lakeformation as _lakeformation
)

import json

class CdkpyStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
       
        with open('./props/tasksetting.json', 'r') as f1:
            py_json1 = json.load(f1)
            ts = json.dumps(py_json1)
            
        # with open('./props/mappingrule.json', 'r') as f2:
        #     py_json2 = json.load(f2)
        #     mr = json.dumps(py_json2)

        with open('./props/config.json', 'r') as f2:
            configuration = json.load(f2)

        def getMappingrules(self, table_list):
            rules =[]
            for index, value in enumerate(table_list,1):
                rules.append(
                    {
                        "rule-type": "selection",
                        "rule-id": str(index),
                        "rule-name": str(index),
                        "object-locator": {
                            "schema-name": value['schemaName'],
                            "table-name": value['tableName']
                        },
                        "rule-action": "include",
                        "filters": []
                    }                    
                )   
            mapping_rules ={
                            "rules":rules
                        }
            return json.dumps(mapping_rules)


        # The code that defines your stack goes here
        S3Accessrole = _iam.Role(self, 'dmsrole', assumed_by = _iam.ServicePrincipal('dms.amazonaws.com'),
                                managed_policies = [_iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess')]
                                )

        raw_bucket = s3.Bucket(self, 'rawbucket', bucket_name ='rawbucket-datalake-cdk-oregon')
        raw_bucket.add_lifecycle_rule(enabled = configuration['s3LifecycleRule']['enabled'], expiration = core.Duration.days(configuration['s3LifecycleRule']['expiration']))
                                
        
        #my_table = ddb.Table(self, id ='dunamoTable', table_name = 'testcdktable',
                             #partition_key = ddb.Attribute(name ='lastname',type = ddb.AttributeType.STRING) )

        dl_dms = _dms.CfnReplicationInstance(self, 'dmsreplication',
                                            replication_instance_class = configuration['DMS_instance_setting']['instance_class'],
                                            replication_instance_identifier = 'datalake-instance-cdk',
                                            allocated_storage= configuration['DMS_instance_setting']['allocated_storage']
                                            )
        
        source_endpoint = _dms.CfnEndpoint(self,'sourceendpoint',endpoint_type = 'source',
                                                        engine_name = configuration['engineName'],
                                                        database_name = configuration['databaseName'],
                                                        username = configuration['username'],
                                                        password = configuration['password'],
                                                        port = configuration['port'],
                                                        server_name = configuration['serverName'],
                                            )

        target_endpoint = _dms.CfnEndpoint(self, 'targetendpoint', endpoint_type = 'target',
                                            engine_name = 's3',
                                            s3_settings = {'bucketName': raw_bucket.bucket_name,
                                                        'serviceAccessRoleArn': S3Accessrole.role_arn},
                                            extra_connection_attributes = 'dataFormat=parquet'
                                            )

         
        dms_task = _dms.CfnReplicationTask(self, 'data2lake-task',migration_type = 'full-load-and-cdc',
                                           replication_instance_arn = dl_dms.ref,
                                           source_endpoint_arn = source_endpoint.ref,
                                           target_endpoint_arn = target_endpoint.ref,
                                           replication_task_settings = ts,
                                           table_mappings = getMappingrules(self, configuration['tableList'])
                                           )

        


        my_table = ddb.Table(self, id ='dynamoTable', table_name = 'ControllerTable',
                             partition_key = ddb.Attribute(name ='path',type = ddb.AttributeType.STRING),
                             billing_mode= ddb.BillingMode.PAY_PER_REQUEST)


        datalake_bucket = s3.Bucket(self, 'datalakebucket', bucket_name = 'datalake-bucket-cdk-oregon')


        glue_role = _iam.Role(self, 'gluerole',assumed_by = _iam.ServicePrincipal('glue.amazonaws.com'),
                                managed_policies=[_iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')])

        raw_bucket.grant_read(glue_role)
        datalake_bucket.grant_read_write(glue_role)

#lake formation settings
#If you have attached managed policy ('AWSLakeFormationDataAdmin') to your own iam user, you should change that policy to allow "lakeformation:PutDataLakeSettings",
#so that the lake setting can be allowed by below code in cdk.
        lake_admin_setting = _lakeformation.CfnDataLakeSettings(self, 'data-lake-GrantAdmin', 
                                                            admins =[_lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                                                                data_lake_principal_identifier = configuration['executiveArn'])]
                                                        ) 
                                                                 
        glue_database = _glue.Database(self, 'gluedatabase',database_name = 'data_lake_gluedb')

        glue_database.node.add_dependency(lake_admin_setting)

        glue_role_permission_inLakeFormation = _lakeformation.CfnPermissions(self, 'permission-glueRole',
                                                                                data_lake_principal = _lakeformation.CfnPermissions.DataLakePrincipalProperty(
                                                                                                        data_lake_principal_identifier = glue_role.role_arn
                                                                                                        ),
                                                                                resource = _lakeformation.CfnPermissions.ResourceProperty(database_resource = _lakeformation.CfnPermissions.DatabaseResourceProperty(name = glue_database.database_name)),
                                                                                permissions = ['ALL'])

        crawler = _glue.CfnCrawler(self, 'datalakecrawler',name = 'Crawler-datalake-cdk' ,role = glue_role.role_arn,
                                targets = {
                                    's3Targets':[
                                        {
                                            'path':'s3://'+ datalake_bucket.bucket_name +'/datalake/'
                                        }
                                    ]
                                },
                                database_name = 'data_lake_gluedb',
                                configuration = "{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"},\"Tables\":{\"AddOrUpdateBehavior\":\"MergeNewColumns\"}}}")
 
        initialload_script = S3Assets.Asset(self, 'initial-load-code',path = './Gluejob/InitialLoad.py')
        incrementalload_script = S3Assets.Asset(self, 'incremental-load-code', path= './Gluejob/IncrementalLoad.py')

        initialload_script.grant_read(glue_role)
        incrementalload_script.grant_read(glue_role)
        my_table.grant_full_access(glue_role)

        initial_load_job = _glue.CfnJob(self, 'initial-job',name = 'InitialLoad-cdk',command=_glue.CfnJob.JobCommandProperty(name= 'glueetl',
                                                                                                    python_version = '3',
                                                                                                    script_location= 's3://' + initialload_script.s3_bucket_name +'/' +initialload_script.s3_object_key
                                                                                                    ),
                                                            role = glue_role.role_arn,
                                                            default_arguments = {
                                                                '--prefix': str(configuration['tableList']),
                                                                '--bucket': raw_bucket.bucket_name,
                                                                '--datalake_bucket': datalake_bucket.bucket_name,
                                                                '--datalake_prefix': 'datalake/',
                                                                '--region': CdkpyStack.of(self).region,
                                                                '--controller_table_name': my_table.table_name
                                                            },
                                                            allocated_capacity= configuration['glue_job_setting']['job_capacity'],
                                                            execution_property=_glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs = configuration['glue_job_setting']['max_concurrent_run_JobExecution'])
                                            )

        incremental_load_job = _glue.CfnJob(self, 'increment-job',name = 'IncrementalLoad-cdk',command=_glue.CfnJob.JobCommandProperty(
                                                                    name = 'glueetl',
                                                                    script_location = 's3://' + incrementalload_script.s3_bucket_name +'/' +incrementalload_script.s3_object_key,
                                                                    python_version = '3'
                                                                ),
                                                                role = glue_role.role_arn,
                                                                default_arguments = {
                                                                    '--prefix': str(configuration['tableList']),
                                                                    '--bucket': raw_bucket.bucket_name,
                                                                    '--datalake_bucket': datalake_bucket.bucket_name,
                                                                    '--datalake_prefix': 'datalake/',
                                                                    '--region': CdkpyStack.of(self).region,
                                                                    '--controller_table_name': my_table.table_name
                                                                },
                                                                allocated_capacity= 2,
                                                                execution_property= _glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs = 1)
                                                )
  

    
        job_trigger = _glue.CfnTrigger(self, 'datalake-glue-trigger',
                                        type = 'SCHEDULED',
                                        schedule = configuration['job_trigger_schedule'],
                                        start_on_creation = False,
                                        actions = [_glue.CfnTrigger.ActionProperty(
                                            job_name = 'IncrementalLoad-cdk'
                                        )
                                        ]
                                     )

        dl_sns = _sns.Topic(self, 'datalake_sns', display_name = 'data-lake-sns')
        
        endpoint_email =configuration['emailSubscriptionList']
    
        for emails in endpoint_email:
            dl_sns.add_subscription(_subscrption.EmailSubscription(emails))

        #Another way to subscribe: dl_subscription = _sns.Subscription(self,'email-subscrption',topic = dl_sns,endpoint='knight.bi@pactera.com',protocol= _sns.SubscriptionProtocol.EMAIL)


        glue_events_target = _events_targets.SnsTopic(dl_sns)
        
        glue_events_rule = _events.Rule(self, 'gluejobevents-datalake', description = 'Using for tracking the failed glue job of data lake',
                                    rule_name = 'dl-gluejob-event',
                                    event_pattern = _events.EventPattern(source = ['aws.glue'],
                                                                        detail_type = ['Glue Job State Change'],
                                                                        detail = {"jobName": [initial_load_job.name],
                                                                                    "state": ["FAILED"]}),
                                    targets =[glue_events_target])
        

        dms_subscription = _dms.CfnEventSubscription(self, 'dmsevents-datalake', sns_topic_arn = dl_sns.topic_arn, 
                                                                                subscription_name = 'datalake-dmsevents',
                                                                                source_type = 'replication-task',
                                                                                event_categories=['failure']
                                                                                )
        #dms_events_rule = _events.Rule(self, 'dmsevents-datalake', description = 'Using for tracking the running status of dms')

#If you have attached managed policy ('AWSLakeFormationDataAdmin') to your own iam user, you should change that policy to allow "lakeformation:PutDataLakeSettings",
#so that the lake setting can be set by below code in cdk.
        # lake_admin_setting = _lakeformation.CfnDataLakeSettings(self, 'data-lake-GrantAdmin', 
        #                                                     admins =[_lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
        #                                                         data_lake_principal_identifier = 'arn:aws:iam::193793567275:user/knight.bi')]
        #                                                 ) 
                                                            
        # glue_role_permission_inLakeFormation = _lakeformation.CfnPermissions(self, 'permission-glueRole',
        #                                                                         data_lake_principal = _lakeformation.CfnPermissions.DataLakePrincipalProperty(
        #                                                                                                 data_lake_principal_identifier = glue_role.role_arn
        #                                                                                                 ),
        #                                                                         resource = _lakeformation.CfnPermissions.ResourceProperty(database_resource = _lakeformation.CfnPermissions.DatabaseResourceProperty(name = 'data_lake_gluedb')),
        #                                                                         permissions = ['ALL'])                                                        
                                                            
                                                                                                                                                                                                                            
#CfnPermissions.DataLakePrincipalProperty(glue_role.role_arn)   permissions = ['ALL'] CfnPermissions.DatabaseResourceProperty
                                                           



