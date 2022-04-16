// Approx 20 min loading
// CSV File at GCP : https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv


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


:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (plantingspace:PlantingSpace {id:row.OBJECTID});

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (overheadutilities:OverheadUtilities {bool:toBoolean(toInteger(row.OverheadUtilities))});

// Create
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (overheadutilities:OverheadUtilities {bool:toBoolean(toInteger(row.OverheadUtilities))})
MERGE (plantingspace)-[:HAS_OVERHEAD_LINES]->(overheadutilities);


// CREATE STATUS
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (currentstatus:CurrentStatus {status:row.PSStatus});

// *** 5 Unique values, try to restrict to 3 only
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (currentstatus:CurrentStatus {status:row.PSStatus})
MERGE (plantingspace)-[:HAVING_STATUS]->(currentstatus);

// CREATE GEOMETRY
// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
// MERGE (geometry:Geometry {point:row.Geometry});

//Create
// :auto USING PERIODIC COMMIT 1000
// LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
// MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
// MATCH (geometry:Geometry {point:row.Geometry})
// MERGE (plantingspace)-[:HAS]->(geometry);

// CREATE JURISDICTION
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (jurisdiction:Jurisdiction {name:row.Jurisdiction});

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (jurisdiction:Jurisdiction {name:row.Jurisdiction})
MERGE (plantingspace)-[:UNDER]->(jurisdiction);

// CREATE LOCATION
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (location:Location {coordinates: row.latitude + "," + row.longitude })
ON CREATE SET
location.latitude = row.latitude, location.longitude = row.longitude;

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (location:Location {coordinates: row.latitude + "," + row.longitude })
MERGE (plantingspace)-[:LOCATED]->(location);


// CREATE DIMESNION
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (dimension:Dimension {dimension: row.Width + "," + row.Length })
ON CREATE SET
dimension.length = row.Length, dimension.width = row.Width;

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (dimension:Dimension {dimension: row.Width + "," + row.Length })
MERGE (plantingspace)-[:HAS]->(dimension);


// CREATE AREA
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (area:Area {NTA: row.NTA });

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (area:Area {NTA: row.NTA })
MERGE (plantingspace)-[:UNDER]->(area);

// CREATE LOCALITY
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (locality:Locality {name: row.Borough });

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (locality:Locality {name: row.Borough })
MERGE (plantingspace)-[:IN]->(locality);

// CREATE SITE TYPE
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (sitetype:SiteType {type: row.PSSite });

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (sitetype:SiteType {type: row.PSSite })
MERGE (plantingspace)-[:TYPE]->(sitetype);

// CREATE INSPECTION
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (inspection:Inspection {id: row.GlobalID })
ON CREATE SET
inspection.createdday = row.Created_Day, inspection.createdmonth = row.Created_Month, inspection.createdyear = row.Created_Year;

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (inspection:Inspection {id: row.GlobalID })
MERGE (plantingspace)<-[:CARRIED_ON]-(inspection);

// CREATE UPDATED
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (updated:Updated {timestamp: row.Updated_Day + "-" + row.Updated_Month + "-" + row.Updated_Year})
ON CREATE SET
updated.updatedday = row.Updated_Day, updated.updatedmonth = row.Updated_Month, updated.updatedyear = row.Updated_Year;

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (plantingspace:PlantingSpace {id:row.OBJECTID})
MATCH (updated:Updated {timestamp: row.Updated_Day + "-" + row.Updated_Month + "-" + row.Updated_Year})
MERGE (plantingspace)-[:UPDATED_ON]->(updated);

// CREATE ZONE
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (zone:Zone {name: row.ParkZone});

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (sitetype:SiteType {type:'Park'})
MATCH (zone:Zone {name: row.ParkZone})
MERGE (sitetype)-[:NAME]->(zone);

// CREATE ADDRESS
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (address:Address {fulladdress:row.Number + "-" + row.Street + "-" + row.CrossStreet1 + "-" + row.CrossStreet2})
ON CREATE SET
address.number = row.Number, address.street = row.Street, address.crossstreet1 = row.CrossStreet1, address.crossstreet2 = row.CrossStreet2;

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (locality:Locality {name: row.Borough })
MATCH (address:Address {fulladdress:row.Number + "-" + row.Street + "-" + row.CrossStreet1 + "-" + row.CrossStreet2})
MERGE (locality)-[:HAS]->(address);

// CREATE COMMUNITYBOARD
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (communityboard:CommunityBoard {name: row.CommunityBoard});

:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (communityboard:CommunityBoard {name: row.CommunityBoard})
MATCH (locality:Locality {name: row.Borough })
MERGE (locality)-[:UNDER]->(communityboard);

// CREATE ZIPCODE
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MERGE (zipcode:Zipcode {postcode: row.Postcode});

// CREATE
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (locality:Locality {name: row.Borough })
MATCH (zipcode:Zipcode {postcode: row.Postcode})
MERGE (locality)-[:HAS]->(zipcode);

// CREATE DISTRICTCODES
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
// MERGE (districtcodes:DistrictCodes {districtdetails: row.CouncilDistrict + "-" + row.StateSenate + "-" + row.StateAssembly + "-" + row.Congressional})
MERGE (districtcodes:DistrictCodes {districtdetails: row.CouncilDistrict})
ON CREATE SET
districtcodes.councildistrict = row.CouncilDistrict, districtcodes.statesenate = row.StateSenate, districtcodes.stateassembly = row.StateAssembly, districtcodes.congressional = row.Congressional;

// CREATE
:auto USING PERIODIC COMMIT 1000
LOAD CSV With HEADERS FROM 'https://storage.googleapis.com/nyc-tree-plantation/Forstry_Planting_Cleaned_Dataset_3_27_2022.csv' AS row
MATCH (districtcodes:DistrictCodes {metadata: row.CouncilDistrict + "-" + row.StateSenate + "-" + row.StateAssembly + "-" + row.Congressional})
MATCH (locality:Locality {name: row.Borough })
MERGE (locality)-[:HAS]->(districtcodes);

