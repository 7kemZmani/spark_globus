// package org.apache.spark

import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

// object Globus {
type Records = (String, Array[String])
type Pair = (String, String)

// def main(args: Array[String]): Unit = {
// 	val sc = new SparkContext(new SparkConf())

def loadContextFile(path: String)(implicit cx: SparkContext = sc): RDD[(String, Iterable[Pair], Int)] = {
	val prep = (line: String) => {
		val s = line.split("\\s", 2)
		val s1 = s(1).trim.split(',')
		(s(0), (s1(0), s1(1)))
	}
	val lines = cx.textFile(path) map prep
	lines.groupByKey map {case (a,b) => (a,b,b.size)}
}

def loadRDD(path: String)(implicit cx: SparkContext = sc): RDD[Records] = {
	val prep = (line: String) => {
		val s = line.split("\\s", 2)
		(s(0), s(1).trim.split(", "))
	}
	cx.textFile(path) map prep
}
def loadVertices(path: String)(implicit cx: SparkContext = sc): RDD[String] = {
	val prep = (line: String) => {
		val s = line.split("\\s", 2)
		s(0)
	}
	cx.textFile(path) map prep
}

def loadEdges(path: String)(implicit cx: SparkContext = sc): RDD[Pair] = {
	val prepLine = (line: String) => {
		val s = line.split("\\s", 2)
		(s(0), s(1).trim.split(", "))
	}

	def prep(line: String): Array[Pair] = {
		val s = prepLine(line)
		s._2 map (n => (s._1, n))
	}

	cx.textFile(path) flatMap prep
}

def loadActivties(path: String)(implicit cx: SparkContext = sc): RDD[(Pair, Double)] = {
	val prep = (line: String) => {
		val c = line.split(',')
		if (c(2) == "nan") ((c(0), c(1)), 0.0)
		else ((c(0), c(1)), c(2).toDouble)
	}
	cx.textFile(path) map prep
}

val initArrayBuff = ArrayBuffer.empty[(String, Double)]
val element2array = (s: ArrayBuffer[(String, Double)], v: (String, Double)) => s += v
val array2array = (s1: ArrayBuffer[(String, Double)], s2: ArrayBuffer[(String, Double)]) => s1 ++= s2

val globalNet = sc.broadcast(loadEdges("ec_net5").collect)
val nodes = loadVertices("ec_net5")
val genes = sc.textFile("mo2")

def initPairs(g: RDD[String] = genes, n: RDD[String] = nodes)(implicit cx: SparkContext = sc): RDD[Pair] = {
	val ns = n.zipWithIndex.map{case (a, b) => (b, a)}
	val gs = g.zipWithIndex.map{case (a, b) => (b, a)}
	gs.join(ns) map {case (a, (x, y)) => (x, y)}
}

val initial_assignments = initPairs()

		// DBs loading:
val homology_gene2ac = sc.broadcast(loadRDD("Homology/modelGeneID2SwissAC.txt").collect)
val homology_ac2ec = sc.broadcast(loadRDD("Homology/modelUniprot2EC.txt").collect)

val orthologs_ec = sc.broadcast(loadRDD("Orthology/geneID2othlogsECs.map").collect)

val pg = sc.broadcast(loadContextFile("Context/Ecoli/zPG_stripped.txt").collect)
val cx = sc.broadcast(loadContextFile("Context/Ecoli/zCX_stripped.txt").collect)
val dc = sc.broadcast(loadContextFile("Context/Ecoli/zDC_stripped.txt").collect)

val modelPG = sc.broadcast(loadContextFile("Context/Model/zPG_stripped.txt").collect)
val modelCX = sc.broadcast(loadContextFile("Context/Model/zCX_stripped.txt").collect)
val modelDC = sc.broadcast(loadContextFile("Context/Model/zDC_stripped.txt").collect)
val modelNeighbors = sc.broadcast(loadEdges("Context/modelGeneNeighbors.txt").collect)

val ec_coocurances = sc.broadcast(loadActivties("EC/newEC_coocurance.txt").collect)

var ranks: RDD[(String, ArrayBuffer[(String, Double)])] = initial_assignments.map(s => (s._1, (s._2, 0.0))).aggregateByKey(initArrayBuff)(element2array, array2array)
var rs = sc.broadcast(ranks.map(s => (s._1, s._2.head._1)).collect)

def neighbor_activities(p: Pair): Array[String] = globalNet.value filter (a => a._1 == p._2) map (a => a._2)
def neighbor_genes(p: Pair, rs: Array[Pair]): Array[String] = {
	val _1 = neighbor_activities(p)
	rs filter (a => _1.contains(a._2)) map (s => s._1)
}

def computeHomology(p: Pair): (Pair, Double) = {
	val _1 = homology_gene2ac.value filter (s => s._1 == p._1)
	if (_1.isEmpty) {
		return (p, 0.0)
	}
	val homologs_nums = _1.head._2.size.toDouble
	val _2 = homology_ac2ec.value filter (s => _1.head._2.contains(s._1))
	if (_2.isEmpty) {
		return (p, 0.0)
	}
	val activities = _2 flatMap (s => s._2) count (a => a == p._2)
	val res = if (activities > 0) (p, activities / homologs_nums) else (p, 0.0)
	res
}

def computeOrthology(p: Pair): (Pair, Double) = {
	val _1 = orthologs_ec.value filter (s => s._1 == p._1)
	if (_1.isEmpty) {
		return (p, 0.0)
	}
	val res = _1 flatMap (s => s._2) filter (a => a == p._2)
	if (res.isEmpty) {
		return (p, 0.0)
	}
	else {
		return (p, 1.0)
	}
}

def countP(b: String, model: Array[(String, Iterable[Pair], Int)]): Double = {
	val bin = model filter (a => a._1 == b) map (a => (a._2, a._3))
	if (bin.isEmpty) return 0.0
	val nbrs = bin.head._1 filter {case (a, b) => modelNeighbors.value.contains((a, b)) || modelNeighbors.value.contains((b, a))}
	if (nbrs.isEmpty) return 0.0
	scala.math.log(nbrs.size.toDouble / bin.head._2)
}

def computeContext(p: Pair): (Pair, Double) = {
	// 1 - create empty 'result'
	// 2 - get the neighbors of the gene
	// 3 - for each method of the 3:
	//    3a - for each neighbor:
	//          a - find the SIZE of 'nbr' & 'p._1' zscore bin.
	//          b - lookup the activites of each model gene from modelGene2EC.
	//          c - find out how many pairs that are neighbors indeed in the model.
	//          d - divid c/a and take the log
	//    3b - compute the sum of logs, and append to 'result'.
	// 4 - return the maxmium value in 'result'
	val nbrs = neighbor_genes(p, rs.value)
	var c1, c2, c3 = 0.0
	nbrs foreach { n => {
		val _1 = pg.value filter (a => a._2.exists({case (a,b) => (a,b) == (p._1, n) || (b,a) == (p._1, n)})) map (a => a._1)
		val _2 = cx.value filter (a => a._2.exists({case (a,b) => (a,b) == (p._1, n) || (b,a) == (p._1, n)})) map (a => a._1)
		val _3 = dc.value filter (a => a._2.exists({case (a,b) => (a,b) == (p._1, n) || (b,a) == (p._1, n)})) map (a => a._1)

		if (_1.isEmpty) c1 += 0.0 else c1 += countP(_1.head, modelPG.value)
		if (_2.isEmpty) c2 += 0.0 else c2 += countP(_2.head, modelCX.value)
		if (_3.isEmpty) c3 += 0.0 else c3 += countP(_3.head, modelDC.value)
	}}

	val mx = List(c1,c2,c3).filterNot(a => a == 0.0)
	if (mx.isEmpty) (p, 0.0) else (p, mx.max)
}

def computeCooccurrence(p: Pair): (Pair, Double) = {
	val nbrs = neighbor_activities(p)
	val res = ArrayBuffer.empty[Double]
	nbrs foreach {n => {
		val _1 = ec_coocurances.value filter {case ((a, b), s) => (a, b) == (p._2, n) || (b, a) == (p._2, n)} map (a => a._2)
		if (_1.isEmpty) res += 0.0
		else res += _1.head
	}}
	val sum = res.sum
	if (sum == 0.0) return (p, 0.0)
	else (p, sum/res.size)
}

// def fitness(p: Pair): RDD[(String, ArrayBuffer[(String, Double)])] = {
	// val ps = nodes map (n => (p._1, n))
	// val others = ranks map (s => (s._1, s._2.head._1)) filter (a => a._1 != p._1)

	// val homology_others = ((others.map(computeHomology)).map(a => ("A", a._2))).reduceByKey(_ + _)
	// val homology_ps = ps.map(computeHomology).cartesian(homology_others).map{case (a, b) => (a._1, a._2 + b._2)}

	// val orthology_others = ((others.map(computeOrthology)).map(a => ("A", a._2))).reduceByKey(_ + _)
	// val orthology_ps = ps.map(computeOrthology).cartesian(orthology_others).map{case (a, b) => (a._1, a._2 + b._2)}

	// val context_others = ((others.map(a => computeContext(a))).map(a => ("A", a._2))).reduceByKey(_ + _)
	// val context_ps = ps.map(a => computeContext(a)).cartesian(context_others).map{case (a, b) => (a._1, a._2 + b._2)}

	// val coEC_others = ((others.map(computeCooccurrence)).map(a => ("A", a._2))).reduceByKey(_ + _)
	// val coEC_ps = ps.map(computeCooccurrence).cartesian(coEC_others).map{case (a, b) => (a._1, a._2 + b._2)}

	// val sum = (((orthology_ps.union(homology_ps)).union(context_ps)).union(coEC_ps)).reduceByKey(_ + _)
	// (sum map {case ((g, a), s) => (g, (a, s))}).aggregateByKey(initArrayBuff)(element2array, array2array) map (s => (s._1, s._2.sortBy(a => -a._2)))
// }

	// start:
// var rs = sc.broadcast(ranks.map(s => (s._1, s._2.head._1)).collect)
// for (_ <- Range(0,1)) {
// 	rs.value foreach {r => {
// 		val ps = nodes map (n => (r._1, n))
// 		val others = ranks map (s => (s._1, s._2.head._1)) filter (a => a._1 != r._1)

// 		val homology_others = ((others.map(computeHomology)).map(a => ("A", a._2))).reduceByKey(_ + _)
// 		val homology_ps = ps.map(computeHomology).cartesian(homology_others).map{case (a, b) => (a._1, a._2 + b._2)}

// 		val orthology_others = ((others.map(computeOrthology)).map(a => ("A", a._2))).reduceByKey(_ + _)
// 		val orthology_ps = ps.map(computeOrthology).cartesian(orthology_others).map{case (a, b) => (a._1, a._2 + b._2)}

// 		val context_others = ((others.map(a => computeContext(a))).map(a => ("A", a._2))).reduceByKey(_ + _)
// 		val context_ps = ps.map(a => computeContext(a)).cartesian(context_others).map{case (a, b) => (a._1, a._2 + b._2)}

// 		val coEC_others = ((others.map(computeCooccurrence)).map(a => ("A", a._2))).reduceByKey(_ + _)
// 		val coEC_ps = ps.map(computeCooccurrence).cartesian(coEC_others).map{case (a, b) => (a._1, a._2 + b._2)}

// 		val sum = (((orthology_ps.union(homology_ps)).union(context_ps)).union(coEC_ps)).reduceByKey(_ + _)
// 		val n = (sum map {case ((g, a), s) => (g, (a, s))}).aggregateByKey(initArrayBuff)(element2array, array2array) map (s => (s._1, s._2.sortBy(a => -a._2)))
// 		// val n = fitness(r)
// 		val ns = ranks filter (s => s._1 != r._1)
// 		ranks = ns.union(n)
// 	}}
// 	rs = sc.broadcast(ranks.map(s => (s._1, s._2.head._1)).collect)
// }
// ranks.saveAsTextFile(args(1))
// sc.stop()
// }
// }
