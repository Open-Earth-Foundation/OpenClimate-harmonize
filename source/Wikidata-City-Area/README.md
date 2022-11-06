# Dataset

Extracted from Wikidata on 5 Nov 2022 by Evan Prodromou <evan@openearth.org>

SPARQL query is:

```
#title: Regions with areas
SELECT DISTINCT ?city ?locode ?area
WHERE
{

  ?city p:P1937 ?statement0.
  ?statement0 (ps:P1937) ?locode.

  ?city p:P2046 ?statement1.
  ?statement1 (psv:P2046/wikibase:quantityAmount) ?area.

}
ORDER BY ?city
```