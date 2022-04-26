// Load NYC Tree Plantation data into Neo4j Graph DB
// 18 April 2022
// Contact: anand.pi@northeastern.edu

// Set parameter for CSV Data File Source
:param csv_file => 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv';

CREATE CONSTRAINT ON (plantingspace:PlantingSpace) ASSERT plantingspace.id IS UNIQUE;
CREATE CONSTRAINT ON (area:Area) ASSERT area.NTA IS UNIQUE;
CREATE CONSTRAINT ON (overheadutilities:OverheadUtilities) ASSERT overheadutilities.bool IS UNIQUE;
CREATE CONSTRAINT ON (currentstatus:CurrentStatus) ASSERT currentstatus.status IS UNIQUE;
CREATE CONSTRAINT ON (jurisdiction:Jurisdiction) ASSERT jurisdiction.name IS UNIQUE;
CREATE CONSTRAINT ON (location:Location) ASSERT location.coordinates IS UNIQUE;
CREATE CONSTRAINT ON (locality:Locality) ASSERT locality.name IS UNIQUE;
CREATE CONSTRAINT ON (sitetype:SiteType) ASSERT sitetype.type IS UNIQUE;
CREATE CONSTRAINT ON (inspection:Inspection) ASSERT inspection.id IS UNIQUE;
CREATE CONSTRAINT ON (updated:Updated) ASSERT updated.timestamp IS UNIQUE;
CREATE CONSTRAINT ON (zone:Zone) ASSERT zone.name IS UNIQUE;
CREATE CONSTRAINT ON (address:Address) ASSERT address.fulladdress IS UNIQUE;
CREATE CONSTRAINT ON (communityboard:CommunityBoard) ASSERT communityboard.name IS UNIQUE;
CREATE CONSTRAINT ON (zipcode:Zipcode) ASSERT zipcode.postcode IS UNIQUE;
CREATE CONSTRAINT ON (dimension:Dimension ) ASSERT dimension.dimension IS UNIQUE;
CREATE CONSTRAINT ON (districtcodes:DistrictCodes) ASSERT districtcodes.districtdetails IS UNIQUE;
CREATE CONSTRAINT ON (createdday:CreatedDay) ASSERT createdday.day IS UNIQUE;
CREATE CONSTRAINT ON (createdmonth:CreatedMonth) ASSERT createdmonth.date IS UNIQUE;
CREATE CONSTRAINT ON (createdyear:CreatedYear) ASSERT createdyear.year IS UNIQUE;
CREATE CONSTRAINT ON (updatedday:UpdatedDay) ASSERT updatedday.day IS UNIQUE;
CREATE CONSTRAINT ON (updatedmonth:UpdatedMonth) ASSERT updatedmonth.date IS UNIQUE;
CREATE CONSTRAINT ON (updatedyear:UpdatedYear) ASSERT updatedyear.year IS UNIQUE;
CREATE CONSTRAINT ON (councildistrict:CouncilDistrict) ASSERT councildistrict.name IS UNIQUE;
CREATE CONSTRAINT ON (statesenate:StateSenate) ASSERT statesenate.name IS UNIQUE;
CREATE CONSTRAINT ON (stateassembly:StateAssembly) ASSERT stateassembly.name IS UNIQUE;
CREATE CONSTRAINT ON (congressional:Congressional) ASSERT congressional.name IS UNIQUE;

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MERGE (plantingspace:PlantingSpace {id:row.OBJECTID})
ON CREATE SET
plantingspace.currentstatus = row.PSStatus,
plantingspace.geometry = row.Geometry,
plantingspace.latitude = row.latitude,
plantingspace.longitude = row.longitude,
plantingspace.overheadutilities = toBoolean(toInteger(row.OverheadUtilities)),
plantingspace.length = row.Length,
plantingspace.width = row.Width;

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (overheadutilities:OverheadUtilities {bool:toBoolean(toInteger(row.OverheadUtilities))});

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
// MATCH (overheadutilities:OverheadUtilities {bool:toBoolean(toInteger(row.OverheadUtilities))})
// MERGE (plantingspace)-[:HAS_OVERHEAD_LINES]->(overheadutilities);

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MERGE (inspection:Inspection {id: row.GlobalID })
ON CREATE SET
inspection.createdday = row.Created_Day,
inspection.createdmonth = row.Created_Month,
inspection.createdyear = row.Created_Year,
inspection.updatedday = row.Updated_Day,
inspection.updatedmonth = row.Updated_Month,
inspection.updatedyear = row.Updated_Year;

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (inspection:Inspection {id: row.GlobalID })
MERGE (plantingspace)<-[:CARRIED_ON]-(inspection);

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (createdday:CreatedDay {day: row.Created_Day });

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (createdmonth:CreatedMonth {month: row.Created_Month });

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (createdyear:CreatedYear {year: row.Created_Year });

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (createdday:CreatedDay {day:row.Created_Day})
// MATCH (inspection:Inspection {id: row.GlobalID })
// MERGE (createdday)<-[:CREATED_ON]-(inspection);

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (createdmonth:CreatedMonth {month:row.Created_Month})
// MATCH (inspection:Inspection {id: row.GlobalID })
// MERGE (createdmonth)<-[:CREATED_ON]-(inspection);

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (createdyear:CreatedYear  {year:row.Created_Year})
// MATCH (inspection:Inspection {id: row.GlobalID })
// MERGE (createdyear)<-[:CREATED_ON]-(inspection);

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (updatedday:UpdatedDay {day: row.Updated_Day });

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (updatedmonth:UpdatedMonth {month: row.Updated_Month });

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (updatedyear:UpdatedYear {year: row.Updated_Year });

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (updatedday:UpdatedDay {day:row.Updated_Day})
// MATCH (inspection:Inspection {id: row.GlobalID })
// MERGE (updatedday)<-[:UPDATED_ON]-(inspection);

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (updatedmonth:UpdatedMonth {month:row.Updated_Month})
// MATCH (inspection:Inspection {id: row.GlobalID })
// MERGE (updatedmonth)<-[:UPDATED_ON]-(inspection);

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (updatedyear:UpdatedYear  {year:row.Updated_Year})
// MATCH (inspection:Inspection {id: row.GlobalID })
// MERGE (updatedyear)<-[:UPDATED_ON]-(inspection);


:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (inspection:Inspection {id: row.GlobalID })
MERGE (plantingspace)<-[:CARRIED_ON]-(inspection);

// CREATE JURISDICTION
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MERGE (jurisdiction:Jurisdiction {name:row.Jurisdiction});

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (jurisdiction:Jurisdiction {name:row.Jurisdiction})
MERGE (plantingspace)-[:UNDER]->(jurisdiction);

// CREATE DIMESNION
// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (dimension:Dimension {dimension: row.Width + "," + row.Length })
// ON CREATE SET
// dimension.length = row.Length, dimension.width = row.Width;

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
// MATCH (dimension:Dimension {dimension: row.Width + "," + row.Length })
// MERGE (plantingspace)-[:HAS]->(dimension);

// CREATE AREA
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MERGE (area:Area {NTA: row.NTA });

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (area:Area {NTA: row.NTA })
MERGE (plantingspace)-[:UNDER]->(area);

// CREATE SITE TYPE
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MERGE (sitetype:SiteType {type: row.PSSite });

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (sitetype:SiteType {type: row.PSSite })
MERGE (plantingspace)-[:TYPE]->(sitetype);

// CREATE ZONE
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MERGE (zone:Zone {name: row.ParkZone});

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MATCH (sitetype:SiteType {type:'Park'})
MATCH (zone:Zone {name: row.ParkZone})
MERGE (sitetype)-[:NAME]->(zone);

// CREATE LOCALITY
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MERGE (locality:Locality {name: row.Borough })
ON CREATE SET
locality.communityboard = row.CommunityBoard,
locality.councildistrict = row.CouncilDistrict,
locality.statesenate = row.StateSenate,
locality.stateassembly = row.StateAssembly,
locality.congressional = row.Congressional;


:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (locality:Locality {name: row.Borough })
MERGE (plantingspace)-[:IN]->(locality);

// CREATE ADDRESS
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MERGE (address:Address {fulladdress:row.Number + "-" + row.Street + "-" + row.CrossStreet1 + "-" + row.CrossStreet2})
ON CREATE SET
address.number = row.Number, address.street = row.Street, address.crossstreet1 = row.CrossStreet1, address.crossstreet2 = row.CrossStreet2;

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MATCH (locality:Locality {name: row.Borough })
MATCH (address:Address {fulladdress:row.Number + "-" + row.Street + "-" + row.CrossStreet1 + "-" + row.CrossStreet2})
MERGE (locality)-[:HAS]->(address);

// CREATE COMMUNITYBOARD
// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (communityboard:CommunityBoard {name: row.CommunityBoard});

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (communityboard:CommunityBoard {name: row.CommunityBoard})
// MATCH (locality:Locality {name: row.Borough })
// MERGE (locality)-[:UNDER]->(communityboard);

// CREATE ZIPCODE
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MERGE (zipcode:Zipcode {postcode: row.Postcode});


:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM $csv_file AS row
MATCH (locality:Locality {name: row.Borough })
MATCH (zipcode:Zipcode {postcode: row.Postcode})
MERGE (locality)-[:HAS]->(zipcode);

// CREATE COUNCILDISTRICT
// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (councildistrict:CouncilDistrict {name: row.CouncilDistrict});

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (councildistrict:CouncilDistrict {name: row.CouncilDistrict})
// MATCH (locality:Locality {name: row.Borough })
// MERGE (locality)-[:UNDER]->(councildistrict);

// // CREATE STATESENATE
// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (statesenate:StateSenate {name: row.StateSenate});

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (statesenate:StateSenate {name: row.StateSenate})
// MATCH (locality:Locality {name: row.Borough })
// MERGE (locality)-[:UNDER]->(statesenate);

// // CREATE StateAssembly
// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (stateassembly:StateAssembly {name: row.StateAssembly});

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (stateassembly:StateAssembly {name: row.StateAssembly})
// MATCH (locality:Locality {name: row.Borough })
// MERGE (locality)-[:UNDER]->(stateassembly);

// // CREATE Congressional
// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MERGE (congressional:Congressional {name: row.Congressional});

// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM $csv_file AS row
// MATCH (congressional:Congressional {name: row.Congressional})
// MATCH (locality:Locality {name: row.Borough })
// MERGE (locality)-[:UNDER]->(congressional);