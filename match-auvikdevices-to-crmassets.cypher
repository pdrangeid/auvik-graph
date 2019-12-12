//MATCH (ad:Auvikdevice)-[r]-(ca:Crmasset) DETACH DELETE r;

// This script attempts to match (:Auvikdevice) to CommitCRM Assets (:Crmasset) using
// multiple approaches.  serial number, mac address, and variants of the devicename

// MATCH Crmassets (serial of auvik device)
MATCH (at:Auviktenant)--(:Auviktenant)--(a:Company) where at.adcount >0
WITH collect(distinct a) as companies
UNWIND companies as a
MATCH (a)--(ca:Crmasset) where exists(ca.serial)
WITH a,collect(distinct ca) as assets
MATCH (at:Auviktenant)--()--(a) where at.adcount >0
WITH assets,a,collect(distinct at) as tenants
UNWIND tenants as at
MATCH (ad:Auvikdevice)--(at)--(:Auviktenant)--(a) where not (ad)--(:Crmasset) and exists(ad.serialnumber)
UNWIND assets as ca
WITH ca,ad where ca.serial=ad.serialnumber
MERGE (ad)<-[:IS_AUVIK_MONITORED]-(ca)
RETURN ad.id,ca.name order by ad.name;


// MATCH Crmassets (mac of auvik device)
MATCH (at:Auviktenant)--(:Auviktenant)--(a:Company) where at.adcount >0
WITH collect(distinct a) as companies
UNWIND companies as a
MATCH (a)--(ca:Crmasset) where exists(ca.mac)
WITH a,collect(distinct ca) as assets
MATCH (at:Auviktenant)--()--(a) where at.adcount >0
WITH assets,a,collect(distinct at) as tenants
UNWIND tenants as at
MATCH (ad:Auvikdevice)--(at)--(:Auviktenant)--(a) where not (ad)--(:Crmasset) and exists(ad.macaddress)
UNWIND assets as ca
WITH ca,ad where ca.mac=ad.macaddress
MERGE (ad)<-[:IS_AUVIK_MONITORED]-(ca)
RETURN ad.id,ca.name order by ad.name;

// MATCH Crmassets (FQname of auvik device)
MATCH (ad:Auvikdevice)--(at:Auviktenant)--(:Auviktenant)--(a:Company) where not (ad)--(:Crmasset)
MATCH (a)--(:Auviktenant)--(at)--(ad) where not (ad)--(:Crmasset) and exists(ad.devicename) and ad.devicename contains '.' and not(ad.devicename contains 'Device@')
MATCH (ad)--(at)--(:Auviktenant)--(a)--(ca:Crmasset) where exists(ca.name) and ca.name contains '.' and not(ca.name contains 'Device@')
WITH at,a,collect(distinct ad) as auvikdevices,collect(distinct ca) as crmassets
UNWIND auvikdevices as ad
UNWIND crmassets as ca
WITH ad,ca,"[^a-zA-Z\\d]" as regex
WITH ad,ca,toLower(apoc.text.replace(ca.name, regex,"")) as caname,toLower(apoc.text.replace(ad.devicename, regex,"")) as adname
WITH ad,ca,caname,adname where caname=adname
MERGE (ad)<-[:IS_AUVIK_MONITORED]-(ca)
RETURN distinct ad.id,ad.devicename,ca.name order by ad.devicename;

// MATCH Crmassets (name of auvik device)
MATCH (at:Auviktenant)--(:Auviktenant)--(a:Company) where at.adcount >0
WITH collect(distinct a) as companies
UNWIND companies as a
MATCH (a)--(ca:Crmasset) where exists(ca.name) and not(ca.name contains 'Device@' or ca.name contains 'Unknown')
WITH a,collect(distinct ca) as assets
MATCH (at:Auviktenant)--()--(a) where at.adcount >0
WITH assets,a,collect(distinct at) as tenants
UNWIND tenants as at
MATCH (ad:Auvikdevice)--(at)--(:Auviktenant)--(a) where not (ad)--(:Crmasset) and exists(ad.devicename) and not(ad.devicename contains 'Device@' or ad.devicename='Unknown')
UNWIND assets as ca
WITH ad,ca,"[^a-zA-Z\\d]" as regex
WITH ad,ca,toLower(apoc.text.replace(ca.name, regex,"")) as caname,toLower(apoc.text.replace(ad.devicename, regex,"")) as adname
WITH ad,ca,caname,adname where caname=adname
MERGE (ad)<-[:IS_AUVIK_MONITORED]-(ca)
RETURN distinct ad.id,ad.devicename,ca.name order by ad.devicename;

// MATCH Crmassets (fq to nonfqname of auvik device)
MATCH (at:Auviktenant)--(:Auviktenant)--(a:Company) where at.adcount >0
WITH collect(distinct a) as companies
UNWIND companies as a
MATCH (a)--(ca:Crmasset) where exists(ca.name) and not(ca.name contains 'Device@' or ca.name contains 'Unknown')
WITH a,collect(distinct ca) as assets
MATCH (at:Auviktenant)--()--(a) where at.adcount >0
WITH assets,a,collect(distinct at) as tenants
UNWIND tenants as at
MATCH (ad:Auvikdevice)--(at)--(:Auviktenant)--(a) where not (ad)--(:Crmasset) and exists(ad.devicename) and not(ad.devicename contains 'Device@' or ad.devicename='Unknown')
UNWIND assets as ca
WITH ad,ca,"[^a-zA-Z\\d]" as regex
WITH ad,ca,toLower(apoc.text.replace(ca.name, regex,"")) as caname,toLower(apoc.text.replace(ad.devicename, regex,"")) as adname
WITH ad,ca,caname,adname where toLower(trim(split(ad.devicename,'.')[0]))=toLower(trim(split(ca.name,'.')[0]))
MERGE (ad)<-[:IS_AUVIK_MONITORED]-(ca)
RETURN distinct ad.id,ad.devicename,ca.name order by ad.devicename;

// MATCH Crmassets (name of auvik device including Device@)
MATCH (at:Auviktenant)--(:Auviktenant)--(a:Company) where at.adcount >0
WITH collect(distinct a) as companies
UNWIND companies as a
MATCH (a)--(ca:Crmasset) where exists(ca.name) and not(ca.name contains 'Unknown')
WITH a,collect(distinct ca) as assets
MATCH (at:Auviktenant)--()--(a) where at.adcount >0
WITH assets,a,collect(distinct at) as tenants
UNWIND tenants as at
MATCH (ad:Auvikdevice)--(at)--(:Auviktenant)--(a) where not (ad)--(:Crmasset) and exists(ad.devicename) and not(ad.devicename='Unknown')
UNWIND assets as ca
WITH ad,ca,"[^a-zA-Z\\d]" as regex
WITH ad,ca,toLower(apoc.text.replace(ca.name, regex,"")) as caname,toLower(apoc.text.replace(ad.devicename, regex,"")) as adname
WITH ad,ca,caname,adname where caname=adname
MERGE (ad)<-[:IS_AUVIK_MONITORED]-(ca)
RETURN distinct ad.id,ad.devicename,ca.name order by ad.devicename;

// FIND Auvik devices without a :Crmasset for a tenant
WITH 'cumm' as matchstring
MATCH (a:Company) where toLower(a.name) contains matchstring
MATCH (ad:Auvikdevice)--(at:Auviktenant)--(:Auviktenant)--(a) where not (ad)--(:Crmasset)
OPTIONAL MATCH (ad)--(dt:Auvikdevicetype)
OPTIONAL MATCH (ad)--(an:Auviknetwork)
return ad.devicename,ad.description,ad.model,dt.name,ad.macaddress,ad.serialnumber,at.name order by ad.devicename
//RETURN ad,dt,an,at