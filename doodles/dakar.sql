CREATE TABLE "teams" (
    "Year" INTEGER,
    "Bib" INTEGER,
    "Team" TEXT,
    PRIMARY KEY ("Year","Bib")
);


CREATE TABLE "crew" (
    "Year" INTEGER,
    "Bib" INTEGER,
    "Num" INTEGER,
    "Name" TEXT,
    "Country" TEXT,
    PRIMARY KEY ("Year","Bib","Num")
);

CREATE TABLE "vehicles" (
    "Year" INTEGER,
    "Bib" INTEGER,
    "VehicleType" TEXT,
    "Brand" TEXT,
    PRIMARY KEY ("Year","Bib")
);


CREATE TABLE "stagestats" (
    "Year" INTEGER,
    "Stage" INTEGER,
    "Start" TEXT,
    "Liaison" TEXT,
    "Special" TEXT,
    "AtStart" INTEGER,
    "Left" INTEGER,
    "Arrived" INTEGER,
    "LatestWP" TEXT,
    "LeaderLatestWP" TEXT,
    "NumLatestWP" TEXT,
    "BibLatestWP" INTEGER,
    "NameLatestWP" TEXT,
    "Vehicle" TEXT,
    "StageDist" INTEGER,
    PRIMARY KEY ("Year","Stage","Vehicle")
);


CREATE TABLE "ranking" (
    "Year" INTEGER,
    "Stage" INTEGER,
    "Type" TEXT, --timerank
    "Pos" INTEGER,
    "Bib" INTEGER,
    "VehicleType" TEXT,
    "Crew" TEXT,
    "Brand" TEXT,
    "Time_raw" TEXT,
    "TimeInS" INTEGER,
    "Gap_raw" TEXT,
    "GapInS" INTEGER,
    "Penalty_raw" TEXT,
    "PenaltyInS" INTEGER,
    PRIMARY KEY ("Year", "Stage", "Type", "Bib")
);

--CREATE TABLE "stagemeta" (
--    "Year" INTEGER,
--    "Stage" INTEGER,
--    "Bib" INTEGER,
--    "RoadPos" REAL,
--    "Refuel" TEXT,
--    PRIMARY KEY ("Year","Stage","Bib")
--);

CREATE TABLE "waypoints" (
    "Year" INTEGER,
    "Stage" INTEGER,
    "Bib" INTEGER,
    "Pos" INTEGER,
    "Waypoint" TEXT,
    "WaypointOrder" INTEGER,
    "WaypointRank" INTEGER, --rank thus far on the stage
    "WaypointPos" INTEGER, --performance between two splits
    "Time_raw" TEXT,
    "TimeInS" INTEGER,
    "Gap_raw" TEXT,
    "GapInS" INTEGER,
    "WaypointDist" INTEGER,
    "VehicleType" TEXT,
    PRIMARY KEY ("Year", "Stage", "Bib", "Waypoint")
);

--there is also a fuller waypoints dataset with kind='full' results, eg ito pos at each split

