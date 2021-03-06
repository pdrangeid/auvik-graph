// SECTION create (:Auviktenant) nodes and relationships to parent and (:Company)

WITH "base-auvik-api-url/tenants/detail?tenantDomainPrefix=bluenetinc" as url,"auvik-restapi-token" as token
CALL apoc.load.jsonParams(url,{Authorization:"Basic "+token,Accept:"application/json"},null) yield value
unwind value.data as tenant
OPTIONAL MATCH (a:Company) where toLower(a.name) = toLower(tenant.attributes.displayName)
MERGE (at:Auviktenant {id:tenant.id})
SET at.domainprefix=tenant.attributes.domainPrefix,at.name=tenant.attributes.displayName,at.owner=tenant.attributes.subscriptionOwner,at.type=tenant.attributes.tenantType
FOREACH (ignoreMe in CASE WHEN exists(a.name) THEN [1] ELSE [] END | MERGE (at)-[:AUVIK_SITE_FOR]->(a))
WITH *
MATCH (pt:Auviktenant {id:tenant.relationships.parent.data.id})
MERGE (at)-[:CHILD_OF_TENANT]->(pt)
return tenant.id,tenant.attributes.displayName,tenant.relationships.parent.data.id;

// SECTION set each tenant to latest update
MATCH (at:Auviktenant)--(an:Auviknetwork)
WITH at,max(an.modified) as latestmod where latestmod >=at.lastupdate or not exists(at.lastupdate)
SET at.lastupdate=latestmod;

// SECTION SET URL property for each :Auviktenant to query networks
WITH "2019-01-01T00:00:00.000Z" as thismorning
MATCH (at:Auviktenant) where not at.type='multiClient'
MERGE (import:Auvikimport {id: 0}) SET import.urllist=[] REMOVE import.url,import.urllist
WITH *,"base-auvik-api-url/inventory/network/info?filter[modifiedAfter]="+coalesce(at.lastupdate,thismorning)+"&tenants="+at.id as url
FOREACH (ignoreMe in CASE WHEN not url in coalesce(import.urllist,[]) then [1] ELSE [] END | SET import.urllist=coalesce(import.urllist,[]) + url)
SET import.pages=0,import.ancount=0
RETURN import.urllist;

// SECTION iterate through the entire tenant device URL list and create :Auviknetwork nodes
WITH *,'auvik-restapi-token' as token
WITH *,'{Authorization:"Basic '+token+'",Accept:"application/json"}' as theparams
MATCH (import:Auvikimport {id: 0}) 
UNWIND import.urllist as tenanturl
CALL apoc.periodic.commit("MATCH (import:Auvikimport {id:0})
WITH import,'"+tenanturl+"' as firsturl
WITH import,coalesce(import.url,firsturl) as theurl
SET import.pages=import.pages+1 REMOVE import.url
WITH import,theurl where trim(theurl)<>''
CALL apoc.load.jsonParams(theurl,"+theparams+",null) YIELD value AS value
WITH import,value
UNWIND value.data as network
WITH import,value,network,split(apoc.text.base64Decode(network.id),',')[0] as tenantid,split(apoc.text.base64Decode(network.id),',')[1] as networkid
MATCH (t:Auviktenant {id:tenantid})
MERGE (nt:Auviknetworktype {name:coalesce(network.attributes.networkType,'None Provided')})
MERGE (an:Auviknetwork {id:networkid,tenant:tenantid})
SET an.name=network.attributes.networkName,an.description=network.attributes.description,an.type=network.attributes.networkType,an.modified=network.attributes.lastModified
SET import.ancount=import.ancount+1
MERGE (an)-[:NETWORK_TYPE]->(nt)
MERGE (an)-[nr:NETWORK_WITHIN_TENANT]->(t)
WITH import,value,an,tenantid LIMIT {limit}
WITH import,value,value.links.next as nextpage
SET import.totalpages=value.meta.totalPages,import.url=nextpage
FOREACH (ignoreMe in CASE WHEN nextpage IS NOT NULL and trim(nextpage) <> '' then [1] ELSE [] END | SET import.url=nextpage)
WITH import,value,nextpage, CASE WHEN nextpage is null then 0 ELSE 1 END AS count
FOREACH(_ in CASE WHEN count = 0 THEN [] ELSE [1] END | SET import.url = nextpage)
RETURN count"
,
{limit:10000}) YIELD batches,batchErrors,failedBatches,failedCommits,commitErrors,executions,runtime,wasTerminated
RETURN *;

// REMOVE (:Auvikdevice) that is over 10 days old
WITH timestamp()-864000000 as tendaysold
MATCH (ad:Auvikdevice) where datetime(ad.modified).epochmillis <tendaysold
WITH ad where ad.lastseen <tendaysold or not exists(ad.lastseen)
DETACH DELETE ad;

MATCH (at:Auviktenant)
OPTIONAL MATCH (at)--(an:Auviknetwork) REMOVE at.ancount
WITH at,count(an) as ancount
SET at.ancount=ancount
return at.name,at.owner,at.ancount,at.devicecount,at.ifcount;

// SECTION set each tenant to latest update
MATCH (at:Auviktenant)--(ad:Auvikdevice)
WITH at,max(ad.modified) as latestmod where latestmod >=at.lastupdate or not exists(at.lastupdate)
SET at.lastupdate=latestmod;

// SECTION SET URL property for each :Auviktenant to query devices
WITH apoc.date.format(timestamp(),'ms',"yyyy-MM-dd'T'00:00:00.000'Z'") as thismorning
MATCH (at:Auviktenant) where not at.type='multiClient' and at.ancount >0
MERGE (import:Auvikimport {id: 1}) SET import.urllist=[] REMOVE import.url
WITH import,at,thismorning
unwind at.id as tenantid
WITH import,at,"base-auvik-api-url/inventory/device/info?tenants="+tenantid+"&filter[modifiedAfter]="+coalesce(at.lastupdate,thismorning)+"&include=deviceDetail" as url
SET at.url=url
FOREACH (ignoreMe in CASE WHEN not at.url in coalesce(import.urllist,[]) then [1] ELSE [] END | SET import.urllist=coalesce(import.urllist,[]) + at.url)
RETURN at.url,import;

// SECTION iterate through the entire tenant device URL list and create :Auvikdevice nodes
WITH *,'auvik-restapi-token' as token
WITH *,'{Authorization:"Basic '+token+'",Accept:"application/json"}' as theparams
MATCH (import:Auvikimport {id: 1}) 
UNWIND import.urllist as tenanturl
CALL apoc.periodic.commit("MATCH (import:Auvikimport {id:1})
WITH import,'"+tenanturl+"' as firsturl
WITH import,coalesce(import.url,firsturl) as theurl
SET import.pages=import.pages+1 REMOVE import.url
WITH import,theurl where trim(theurl)<>''
CALL apoc.load.jsonParams(theurl,"+theparams+",null) YIELD value AS value
WITH import,value
UNWIND value.data as device
WITH import,value,device,split(apoc.text.base64Decode(device.id),',')[0] as tenantid,split(apoc.text.base64Decode(device.id),',')[1] as deviceid
MATCH (at:Auviktenant {id:tenantid})
MERGE (adt:Auvikdevicetype {name:coalesce(device.attributes.deviceType,'None Provided')})
MERGE (ad:Auvikdevice {id:deviceid,tenant:tenantid})
SET ad.type=device.attributes.deviceType,ad.model=device.attributes.makeModel,ad.vendor=device.attributes.vendorName,ad.description=device.attributes.description
SET ad.devicename=toLower(trim(apoc.text.replace(device.attributes.deviceName,'[^a-zA-Z0-9]',''))),ad.swversion=device.attributes.softwareVersion,ad.fwversion=device.attributes.firmwareVersion,ad.modified=device.attributes.lastModified
SET ad.serialnumber=toLower(apoc.text.replace(apoc.text.replace(device.attributes.serialNumber, '[^a-zA-Z0-9]',''),'VMware',''))
SET ad.lastseen=datetime(device.attributes.lastSeenTime).epochmillis,ad.managestatus=device.attributes.manageStatus
MERGE (ad)-[:DEVICE_IS_TYPE]->(adt)
MERGE (ad)-[:WITHIN_TENANT]->(at)
WITH import,value,device,ad
UNWIND device.attributes.ipAddresses as deviceip
FOREACH (ignoreMe in CASE WHEN not deviceip in coalesce(ad.ipaddress,[]) then [1] ELSE [] END | SET ad.ipaddress=coalesce(ad.ipaddress,[]) + deviceip)
WITH import,value,ad,device LIMIT {limit}
UNWIND device.relationships.networks as network
OPTIONAL MATCH (an:Auviknetwork {id:split(apoc.text.base64Decode(network.data[0].id),',')[1],tenant:split(apoc.text.base64Decode(network.data[0].id),',')[0]})
FOREACH (ignoreMe in CASE WHEN exists(an.id) then [1] ELSE [] END | MERGE (ad)-[:WITHIN_NETWORK]->(an))
WITH import,value,value.links.next as nextpage
SET import.totalpages=value.meta.totalPages,import.url=nextpage
FOREACH (ignoreMe in CASE WHEN nextpage IS NOT NULL and trim(nextpage) <> '' then [1] ELSE [] END | SET import.url=nextpage)
WITH import,value,nextpage, CASE WHEN nextpage is null then 0 ELSE 1 END AS count
FOREACH(_ in CASE WHEN count = 0 THEN [] ELSE [1] END | SET import.url = nextpage)
RETURN count"
,
{limit:10000}) YIELD batches,batchErrors,failedBatches,failedCommits,commitErrors,executions,runtime,wasTerminated
RETURN *;

// SECTION set each tenant with devicecount
MATCH (at:Auviktenant)--(ad:Auvikdevice) REMOVE at.devicecount
WITH at,count(ad) as adcount
SET at.adcount=adcount
RETURN at.name,adcount;

// SECTION set each tenant to latest update
MATCH (at:Auviktenant) where at.adcount >0
MATCH (at)--(ad:Auvikdevice)
REMOVE at.lastupdate
WITH at,ad
MATCH (ad)--(ai:Auvikinterface)
WITH at,max(ai.modified) as latestmod where latestmod >=at.lastupdate or not exists(at.lastupdate)
SET at.lastupdate=latestmod
RETURN at.name,latestmod;

// SECTION SET URL property for each :Auviktenant to query interfaces
// Builds a list of all tenants with at least 1 network
WITH apoc.date.format(timestamp(),'ms',"yyyy-MM-dd'T'00:00:00.000'Z'") as thismorning
MATCH (at:Auviktenant) where not at.type='multiClient' and at.adcount > 0
MERGE (import:Auvikimport {id: 2}) SET import.urllist=[] REMOVE import.url,import.urllist
WITH *,"base-auvik-api-url/inventory/interface/info?filter[modifiedAfter]="+coalesce(at.lastupdate,thismorning)+"&tenants="+at.id as url
FOREACH (ignoreMe in CASE WHEN not url in coalesce(import.urllist,[]) then [1] ELSE [] END | SET import.urllist=coalesce(import.urllist,[]) + url)
SET import.pages=0,import.ifcount=0
RETURN import.urllist;

// SECTION iterate through the entire tenant device URL list and create :Auvikinterface nodes
WITH *,'auvik-restapi-token' as token
WITH *,'{Authorization:"Basic '+token+'",Accept:"application/json"}' as theparams
MATCH (import:Auvikimport {id: 2}) 
UNWIND import.urllist as tenanturl
CALL apoc.periodic.commit("MATCH (import:Auvikimport {id:2})
WITH import,'"+tenanturl+"' as firsturl
WITH import,coalesce(import.url,firsturl) as theurl
SET import.pages=import.pages+1 REMOVE import.url
WITH import,theurl where trim(theurl)<>''
CALL apoc.load.jsonParams(theurl,"+theparams+",null) YIELD value AS value
WITH import,value
UNWIND value.data as interface
WITH import,value,interface,split(apoc.text.base64Decode(interface.id),',')[0] as tenantid,split(apoc.text.base64Decode(interface.id),',')[1] as interfaceid
OPTIONAL MATCH (at:Auviktenant {id:interface.relationships.tenant.data.id})
MERGE (ai:Auvikinterface {id:interfaceid,tenant:tenantid})
SET ai.speed=interface.attributes.negotiatedSpeed,ai.modified=interface.attributes.lastModified,ai.duplex=interface.attributes.duplex,ai.interfacename=interface.attributes.interfaceName
SET ai.operstatus=interface.attributes.operationalStatus,ai.adminstatus=interface.attributes.adminStatus,ai.parents='none'
SET ai.macaddress=toLower(apoc.text.replace(interface.attributes.macAddress,'[^a-zA-Z0-9]',''))
SET import.ifcount=import.ifcount+1
MERGE (ait:Auvikinterfacetype {name:coalesce(interface.attributes.interfaceType,'None Provided')})
MERGE (ai)-[:INTERFACE_IS_TYPE]->(ait)
WITH import,value,interface,ai,at
UNWIND interface.relationships.parentDevice.data as parent
OPTIONAL MATCH (apd:Auvikdevice {id:split(apoc.text.base64Decode(parent.id),',')[1],tenant:split(apoc.text.base64Decode(parent.id),',')[0]})
FOREACH (ignoreMe in CASE WHEN exists(apd.id) then [1] ELSE [] END | MERGE (ai)-[:PARENT_DEVICE]->(apd))
//FOREACH (ignoreMe in CASE WHEN exists(apd.id) and not exists(apd.macaddress) then [1] ELSE [] END | SET apd.macaddress=ai.macaddress)
FOREACH (ignoreMe in CASE WHEN not exists(apd.id) and exists(at.id) then [1] ELSE [] END | MERGE (ai)-[pir:PARENTLESS_INTERFACE]->(at) SET pir.id=split(apoc.text.base64Decode(parent.id),',')[1])
WITH import,value,ai,interface
UNWIND interface.attributes.ipAddresses as interfaceip
FOREACH (ignoreMe in CASE WHEN not interfaceip in coalesce(ai.ipaddress,[]) then [1] ELSE [] END | SET ai.ipaddress=coalesce(ai.ipaddress,[]) + interfaceip)
WITH import,value,ai,interface LIMIT {limit}
UNWIND interface.relationships.connectedTo.data as conndevice
OPTIONAL MATCH (aci:Auvikinterface {id:split(apoc.text.base64Decode(conndevice.id),',')[1],tenant:split(apoc.text.base64Decode(conndevice.id),',')[0]})
FOREACH (ignoreMe in CASE WHEN exists(aci.id) then [1] ELSE [] END | MERGE (ai)-[:CONNECTED_TO]->(aci))
WITH import,value,value.links.next as nextpage
SET import.totalpages=value.meta.totalPages,import.url=nextpage
FOREACH (ignoreMe in CASE WHEN nextpage IS NOT NULL and trim(nextpage) <> '' then [1] ELSE [] END | SET import.url=nextpage)
WITH import,value,nextpage, CASE WHEN nextpage is null then 0 ELSE 1 END AS count
FOREACH(_ in CASE WHEN count = 0 THEN [] ELSE [1] END | SET import.url = nextpage)
RETURN count"
,
{limit:10000}) YIELD batches,batchErrors,failedBatches,failedCommits,commitErrors,executions,runtime,wasTerminated
RETURN *;

MATCH (at:Auviktenant) where not at.type='multiClient' and at.ancount > 0
OPTIONAL MATCH (at)--(:Auvikdevice)--(ai:Auvikinterface) REMOVE at.ifcount
WITH at,count(ai) as aicount
SET at.ifcount=aicount
return at.name,at.owner,at.adcount,at.ifcount;

// Set the DEVICE macaddress to the me0/tn-mgt0 'Ethernet' Interface macaddress (when there are multiple interfaces with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad)-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,count(r) as intcount
WITH ad,intcount where intcount > 1
MATCH (ad)--(ai)--(ait:Auvikinterfacetype {name:'ethernet'}) where exists(ai.macaddress) and (ai.interfacename ='me0' or ai.interfacename = 'tn-mgt0' or ai.interfacename = 'ManagementEthernet 0/0')
SET ad.macaddress=ai.macaddress
RETURN ad,ai,ait;
//RETURN count(ad);
//return distinct ad.devicename,intcount order by intcount desc

// Set the DEVICE macaddress to the (IF name contains manage or mgmt) 'Ethernet' Interface macaddress (when there are multiple interfaces with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad)-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,count(r) as intcount
WITH ad,intcount where intcount > 1
MATCH (ad)--(ai)--(ait:Auvikinterfacetype {name:'ethernet'}) where exists(ai.macaddress) and (toLower(ai.interfacename) contains 'manage' or toLower(ai.interfacename) contains 'mgmt')
SET ad.macaddress=ai.macaddress
RETURN count(ad);
//return distinct ad.devicename,intcount order by intcount desc

// Set the Mitel DEVICE macaddress to theExternal LAN 'Ethernet' Interface macaddress (when there are multiple interfaces with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad )-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,count(r) as intcount
WITH ad,intcount where intcount > 1
MATCH (ad {vendor:'Mitel'})--(ai {interfacename:'External 10/100 LAN Port'})--(ait:Auvikinterfacetype {name:'ethernet'}) where exists(ai.macaddress)
SET ad.macaddress=ai.macaddress
RETURN count(ad);
//return distinct ad.devicename,intcount order by intcount desc

// Set the HP JetDirect DEVICE macaddress to the Ethernet Interface macaddress (when there are multiple interfaces with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad )-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,count(r) as intcount
WITH ad,intcount where intcount > 1
MATCH (ad {model:'JetDirect'})--(ai {interfacename:'Ethernet'})--(ait:Auvikinterfacetype {name:'ethernet'}) where exists(ai.macaddress)
SET ad.macaddress=ai.macaddress
RETURN count(ad);
//return distinct ad.devicename,intcount order by intcount desc

// Set the Zebra DEVICE macaddress to the SMC Ethernet Interface macaddress (when there are multiple interfaces with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad )-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,count(r) as intcount
WITH ad,intcount where intcount > 1
MATCH (ad {vendor:'Zebra'})--(ai)--(ait:Auvikinterfacetype {name:'ethernet'}) where exists(ai.macaddress) and toLower(ad.model) ends with 'printserver' and ai.interfacename starts with 'SMC'
SET ad.macaddress=ai.macaddress
RETURN count(ad);
//return distinct ad.devicename,intcount order by intcount desc

// Set the VMWARE ESX DEVICE macaddress to vmnic0 'Ethernet' Interface macaddress (when there are multiple interfaces with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad )-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,count(r) as intcount
WITH ad,intcount where intcount > 1
MATCH (ad {model:'ESX'})--(ai)--(ait:Auvikinterfacetype {name:'ethernet'}) where exists(ai.macaddress) and toLower(ai.interfacename) contains 'vmnic0'
SET ad.macaddress=ai.macaddress
RETURN count(ad);
//return distinct ad.devicename,intcount order by intcount desc

// Set the DEVICE macaddress to management 'virtualNic' Interface macaddress (when there are multiple interfaces with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad )-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,count(r) as intcount
WITH ad,intcount where intcount > 1
MATCH (ad)--(ai)--(ait:Auvikinterfacetype {name:'virtualNic'}) where exists(ai.macaddress) and toLower(ai.interfacename) contains 'management'
SET ad.macaddress=ai.macaddress
RETURN count(ad);
//return distinct ad.devicename,intcount order by intcount desc


// Set the DEVICE macaddress to the eth0 'Ethernet' Interface macaddress (when there are multiple interfaces with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad)-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,count(r) as intcount
WITH ad,intcount where intcount > 1
MATCH (ad)--(ai {interfacename:'eth0'})--(ait:Auvikinterfacetype {name:'ethernet'}) where exists(ai.macaddress)
SET ad.macaddress=ai.macaddress
RETURN count(ad);
//return distinct ad.devicename,intcount order by intcount desc

// Set the DEVICE macaddress to the eth0 'Ethernet' Interface macaddress (when there are multiple interfaces with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad)-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,count(r) as intcount
WITH ad,intcount where intcount > 1
MATCH (ad)--(ai)--(ait:Auvikinterfacetype {name:'ethernet'}) where exists(ai.macaddress) and (ai.interfacename='ge-0/0/0' or ai.interfacename='GigabitEthernet 0/0')
SET ad.macaddress=ai.macaddress
RETURN count(ad);
//return distinct ad.devicename,intcount order by intcount desc


// Set the DEVICE macaddress to the Interface macaddress (when there is ONLY 1 interface with a mac address)
MATCH (ad:Auvikdevice) where not exists(ad.macaddress)
MATCH (ad)-[r]-(ai:Auvikinterface) where exists(ai.macaddress)
WITH ad,ai,count(r) as intcount
WITH ad,ai,intcount where intcount = 1
SET ad.macaddress=ai.macaddress
RETURN count(ad);
//return distinct ad.devicename,ai.macaddress,intcount order by intcount desc


// SECTION remove stale devicetype relationships
MATCH (ad:Auvikdevice)-[r]-(adt:Auvikdevicetype) where exists(ad.type) and ad.type <> adt.name
DETACH DELETE r;

// REMOVE (:Auvikdevice) that is not tied to an asset, and is over 1 day old
WITH timestamp()-86400000 as onedayold
MATCH (ad:Auvikdevice) where not (ad)--(:Crmasset) and datetime(ad.modified).epochmillis <onedayold
WITH ad where ad.lastseen <onedayold or not exists(ad.lastseen)
DETACH DELETE ad;