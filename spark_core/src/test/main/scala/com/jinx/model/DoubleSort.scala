package com.jinx.model

class DoubleSort(val first:Double, val second:Double) extends Comparable[DoubleSort] with Serializable {
  override def compareTo(o: DoubleSort): Int = {
    if(this.first!=o.first){
      if (this.first>o.first){
        Math.ceil(this.first-o.first).toInt
      }else {
        Math.floor(this.first-o.first).toInt
      }
    }else {
      (this.second-o.second).toInt
    }
  }


  override def toString = s"DoubleSort($first, $second)"
}
