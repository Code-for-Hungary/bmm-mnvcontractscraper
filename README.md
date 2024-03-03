# MNV szerződés scraper

A [Figyuszhoz](https://figyusz.k-monitor.hu/) készült scraper, ami a [Magyar Nemzeti Vagyonkezelő szerződéskeresőjében](https://www.mnv.hu/gazdalkodas/kozbeszerzesek/szerzodeskereso) keres kulcsszavakat vagy adatfrissítést.

Ez a repo a K-Monitor Bulletin Monitor (BM) projektjéhez fejlesztett MNV szerződés scrapert tartalmazza.  
A szerződések adatait az MNV weboldala által is használt JSON formában tölti le. Egy letöltés az előző letöltés napjától az aktuális napig szűr.

## Telepítés

Telepítsd a szükséges csomagokat.
~~~
pip install -r requirements.txt
~~~
Telepítsd a nyelvi modellt a szótövezéshez.
~~~
python3 install.py
~~~
