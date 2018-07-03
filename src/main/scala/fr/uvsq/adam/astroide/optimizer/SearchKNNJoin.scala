package fr.uvsq.adam.astroide.executor

import adql.query.ADQLObject
import adql.query.from.CrossJoin
import adql.search.SimpleSearchHandler

class SearchKNNJoin extends SimpleSearchHandler(true, false) {

    override def `match`(obj: ADQLObject): Boolean = {
	try {
	    val comp = obj.asInstanceOf[CrossJoin]

	    return true
	}

	catch {
	    case cce: ClassCastException ⇒ false
	}
    }


}
