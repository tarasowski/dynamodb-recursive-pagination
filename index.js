const AWS = require('aws-sdk')
const dynamodb = new AWS.DynamoDB.DocumentClient({region: 'eu-central-1'})
const asyncPipe = (...fns) => x => fns.reduce(async (v, f) => f(await v), x)
const {lensPath, view, map, lensProp, reduce} = require('ramda')

const TABLE_NAME = 'FDHSKUVersions-staging'
const xManufacturer = lensPath(['generalData', 'manufacturerFlavorName']) 

const constructParamsScan = (tableName = TABLE_NAME) => ({
  TableName: tableName,
})

 const onScan = (resolve, reject) => (params = constructParamsScan()) => items =>(err, data) => 
    err
      ? reject(err)
      : data.LastEvaluatedKey
        ? (
            dynamodb.scan({...params, ExclusiveStartKey: data.LastEvaluatedKey},
            onScan(resolve, reject)()([...items, ...data.Items])), console.log('Scanning for more...')
          )
        : !data.LastEvaluatedKey
          ? resolve([...items, ...data.Items])
          : reject('Something went wrong with migration')

const scanDatabase = () =>
  new Promise((resolve, reject) =>
    dynamodb.scan(constructParamsScan(), onScan(resolve, reject)()([]))    
  )

const scan = (params = constructParamsScan()) => () =>
  dynamodb.scan(params).promise()

const filterValues = (a, v) =>
    (v !== undefined && v !== null ? a.concat(v) : a, [])


const pipeline = asyncPipe(
  scanDatabase,
  map(x => view(xManufacturer, x)),
  reduce(filterValues)
)

pipeline()
  .then(x => console.log(JSON.stringify(x, null, 4)))
  .catch(e => console.error(e))

