

select * from kunde;
select * from auftrag;
select * from auftragsposten;

BEGIN;

INSERT INTO Kunde VALUES (7, 'HAZARD-GMBH', 'Willy-Andreas-Alle 3', 76131, 'Karlsruhe', 0);
INSERT INTO Auftrag VALUES (6, '2022-01-13', 7, 1);
INSERT INTO Auftragsposten VALUES (53,6,100001,5,3500);
UPDATE Kunde SET Sperre = 1 WHERE name = 'HAZARD-GMBH';


COMMIT;

select * from kunde;
select * from auftrag;
select * from auftragsposten;
















With hilfstabelle(

Select 
SUM(COOALESCE(Entfernung, 0)*0,3) as pkw_kosten , bahn_kosten from reisen

Where 
);


Select count(mi from Mitarbeiter m 
Join reisen
Where r.start="bad" and r.ziel="KA" and 