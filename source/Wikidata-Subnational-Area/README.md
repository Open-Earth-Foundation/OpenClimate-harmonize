# Dataset

Extracted from Wikidata on 3 Nov 2022 by Evan Prodromou <evan@openearth.org>

SPARQL query is:

```
#title: Regions with areas
SELECT DISTINCT ?region ?iso31662 ?area
WHERE
{

  ?region p:P300 ?statement0.
  ?statement0 (ps:P300) ?iso31662.

  ?region p:P2046 ?statement1.
  ?statement1 (psv:P2046/wikibase:quantityAmount) ?area.

}
ORDER BY ?region
```