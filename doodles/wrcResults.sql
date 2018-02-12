CREATE TABLE "itinerary_event" (
  "eventId" INTEGER,
  "itineraryId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "priority" INTEGER
);
CREATE TABLE "itinerary_legs" (
  "itineraryId" INTEGER,
  "itineraryLegId" INTEGER PRIMARY KEY,
  "legDate" TEXT,
  "name" TEXT,
  "order" INTEGER,
  "startListId" INTEGER,
  "status" TEXT,
  FOREIGN KEY ("itineraryId") REFERENCES "itinerary_event" ("itineraryId")
);
CREATE TABLE "itinerary_sections" (
  "itineraryLegId" INTEGER,
  "itinerarySectionId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "order" INTEGER,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);
CREATE TABLE "itinerary_stages" (
  "code" TEXT,
  "distance" REAL,
  "eventId" INTEGER,
  "name" TEXT,
  "number" INTEGER,
  "stageId" INTEGER PRIMARY KEY,
  "stageType" TEXT,
  "status" TEXT,
  "timingPrecision" TEXT,
  "itineraryLegId" INTEGER,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);
CREATE TABLE "itinerary_controls" (
  "code" TEXT,
  "controlId" INTEGER PRIMARY KEY,
  "distance" REAL,
  "eventId" INTEGER,
  "firstCarDueDateTime" TEXT,
  "firstCarDueDateTimeLocal" TEXT,
  "location" TEXT,
  "stageId" INTEGER,
  "status" TEXT,
  "targetDuration" TEXT,
  "targetDurationMs" INTEGER,
  "timingPrecision" TEXT,
  "type" TEXT,
  "itineraryLegId" INTEGER,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);
CREATE TABLE "startlists" (
  "codriver.abbvName" TEXT,
  "codriver.code" TEXT,
  "codriver.country.countryId" INTEGER,
  "codriver.country.iso2" TEXT,
  "codriver.country.iso3" TEXT,
  "codriver.country.name" TEXT,
  "codriver.countryId" INTEGER,
  "codriver.firstName" TEXT,
  "codriver.fullName" TEXT,
  "codriver.lastName" TEXT,
  "codriver.personId" INTEGER,
  "codriverId" INTEGER,
  "driver.abbvName" TEXT,
  "driver.code" TEXT,
  "driver.country.countryId" INTEGER,
  "driver.country.iso2" TEXT,
  "driver.country.iso3" TEXT,
  "driver.country.name" TEXT,
  "driver.countryId" INTEGER,
  "driver.firstName" TEXT,
  "driver.fullName" TEXT,
  "driver.lastName" TEXT,
  "driver.personId" INTEGER,
  "driverId" INTEGER,
  "eligibility" TEXT,
  "entrant.entrantId" INTEGER,
  "entrant.logoFilename" TEXT,
  "entrant.name" TEXT,
  "entrantId" INTEGER,
  "entryId" INTEGER PRIMARY KEY,
  "eventId" INTEGER,
  "group.name" TEXT,
  "groupId" INTEGER,
  "identifier" TEXT,
  "manufacturer.logoFilename" TEXT,
  "manufacturer.manufacturerId" INTEGER,
  "manufacturer.name" TEXT,
  "manufacturerId" INTEGER,
  "priority" TEXT,
  "status" TEXT,
  "tyreManufacturer" TEXT,
  "vehicleModel" TEXT,
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "startlist_classes" (
  "eventClassId" INTEGER,
  "eventId" INTEGER,
  "name" TEXT,
  "entryId" INTEGER,
  PRIMARY KEY ("eventClassId","entryId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "penalties" (
  "controlId" INTEGER,
  "entryId" INTEGER,
  "penaltyDuration" TEXT,
  "penaltyDurationMs" INTEGER,
  "penaltyId" INTEGER PRIMARY KEY,
  "reason" TEXT,
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "retirements" (
  "controlId" INTEGER,
  "entryId" INTEGER,
  "reason" TEXT,
  "retirementDateTime" TEXT,
  "retirementDateTimeLocal" TEXT,
  "retirementId" INTEGER PRIMARY KEY,
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stagewinners" (
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "stageId" INTEGER,
  "stageName" TEXT,
  PRIMARY KEY ("stageId","entryId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId")
);
CREATE TABLE "stage_overall" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "entryId" INTEGER,
  "penaltyTime" TEXT,
  "penaltyTimeMs" INTEGER,
  "position" INTEGER,
  "stageTime" TEXT,
  "stageTimeMs" INTEGER,
  "totalTime" TEXT,
  "totalTimeMs" INTEGER,
  "stageId" INTEGER,
  PRIMARY KEY ("stageId","entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "split_times" (
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "splitDateTime" TEXT,
  "splitDateTimeLocal" TEXT,
  "splitPointId" INTEGER,
  "splitPointTimeId" INTEGER PRIMARY KEY,
  "startDateTime" TEXT,
  "startDateTimeLocal" TEXT,
  "stageId" INTEGER,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stage_times_stage" (
  "diffFirst" TEXT,
  "diffFirstMs" REAL,
  "diffPrev" TEXT,
  "diffPrevMs" REAL,
  "elapsedDuration" TEXT,
  "elapsedDurationMs" REAL,
  "entryId" INTEGER,
  "position" INTEGER,
  "source" TEXT,
  "stageId" INTEGER,
  "stageTimeId" INTEGER PRIMARY KEY,
  "status" TEXT,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stage_times_overall" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "entryId" INTEGER,
  "penaltyTime" TEXT,
  "penaltyTimeMs" INTEGER,
  "position" INTEGER,
  "stageTime" TEXT,
  "stageTimeMs" INTEGER,
  "totalTime" TEXT,
  "totalTimeMs" INTEGER,
  "stageId" INTEGER,
  PRIMARY KEY ("stageId","entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "championship_lookup" (
  "championshipId" INTEGER PRIMARY KEY,
  "fieldFiveDescription" TEXT,
  "fieldFourDescription" TEXT,
  "fieldOneDescription" TEXT,
  "fieldThreeDescription" TEXT,
  "fieldTwoDescription" TEXT,
  "name" TEXT,
  "seasonId" INTEGER,
  "type" TEXT,
  "_codeClass" TEXT,
  "_codeTyp" TEXT
);
CREATE TABLE "championship_results" (
  "championshipEntryId" INTEGER,
  "championshipId" INTEGER,
  "dropped" INTEGER,
  "eventId" INTEGER,
  "pointsBreakdown" TEXT,
  "position" TEXT,
  "publishedStatus" TEXT,
  "status" TEXT,
  "totalPoints" INTEGER,
  PRIMARY KEY ("championshipEntryId","eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_codrivers" (
  "championshipEntryId" INTEGER PRIMARY KEY ,
  "championshipId" INTEGER,
  "entrantId" TEXT,
  "ManufacturerTyre" TEXT,
  "Manufacturer" TEXT,
  "FirstName" TEXT,
  "CountryISO3" TEXT,
  "LastName" TEXT,
  "manufacturerId" INTEGER,
  "personId" INTEGER,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "championship_manufacturers" (
  "championshipEntryId" INTEGER PRIMARY KEY ,
  "championshipId" INTEGER,
  "entrantId" INTEGER,
  "Name" TEXT,
  "LogoFileName" TEXT,
  "Manufacturer" TEXT,
  "manufacturerId" INTEGER,
  "personId" TEXT,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "championship_rounds" (
  "championshipId" INTEGER,
  "eventId" INTEGER,
  "order" INTEGER,
  PRIMARY KEY ("championshipId","eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_events" (
  "categories" TEXT,
  "country.countryId" INTEGER,
  "country.iso2" TEXT,
  "country.iso3" TEXT,
  "country.name" TEXT,
  "countryId" INTEGER,
  "eventId" INTEGER PRIMARY KEY,
  "finishDate" TEXT,
  "location" TEXT,
  "mode" TEXT,
  "name" TEXT,
  "organiserUrl" TEXT,
  "slug" TEXT,
  "startDate" TEXT,
  "surfaces" TEXT,
  "timeZoneId" TEXT,
  "timeZoneName" TEXT,
  "timeZoneOffset" INTEGER,
  "trackingEventId" INTEGER ,
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_drivers" (
  "championshipEntryId" INTEGER PRIMARY KEY ,
  "championshipId" INTEGER,
  "entrantId" TEXT,
  "ManufacturerTyre" TEXT,
  "Manufacturer" TEXT,
  "FirstName" TEXT,
  "CountryISO3" TEXT,
  "LastName" TEXT,
  "manufacturerId" INTEGER,
  "personId" INTEGER,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);