package eu.stratosphere.emma.streaming.examples

import eu.stratosphere.emma.streaming.api.StreamBag

object LinearRoad {

  case class Input(Type: Int, Time: Int, VID: Int, Spd: Int, XWay: Int, Lane: Int, Dir: Int, Seg: Int, Pos: Int,
                   QID: Int, Sinit: Int, Send: Int, DOW: Int, TOD: Int, Day: Int)

  case class PositionReport(Time: Int, VID: Int, Spd: Int, XWay: Int, Lane: Int, Dir: Int, Seg: Int, Pos: Int)

  // Historical queries
  case class AccountBalanceQ(Time: Int, VID: Int, QID: Int)
  case class DailyExpenditureQ(Time: Int, VID: Int, XWay: Int, QID: Int, Day: Int)
  case class TravelTimeQ(Time: Int, VID: Int, XWay: Int, QID: Int, Sinit: Int, Send: Int, DOW: Int, TOD: Int)

  // Historical Data
  case class TollHistoryEntry(VID: Int, Day: Int, XWay: Int, Tolls: Int)
  case class SegmentHistoryEntry(Day: Int, Min: Int, XWay: Int, Dir: Int, Seg: Int, Lav: Int, Cnt: Int, Toll: Int)

  // Output
  case class TollNotif(VID: Int, Time: Int, Emit: Int, Spd: Int, Toll: Int)


  def entranceLane(l: Int) = l == 0 // (ENTRY)
  def travelLane(l: Int) = l > 0 && l < 4 // (TRAVEL)
  def exitLane(l: Int) = l == 4 // (EXIT)

  def main(input: StreamBag[Input]): Unit = {

    val positionReports = for {
      x <- input
      if x.Type == 0
    } yield PositionReport(x.Time, x.VID, x.Spd, x.XWay, x.Lane, x.Dir, x.Seg, x.Pos)

    val minNumber = (t: Int) => t/60 + 1

    // Note: We can't define this the way it's defined in the paper, because the system doesn't know that
    // vehicles emit exactly one position report every 30 seconds.
    val tollNotifTrigger = for{
      p <- positionReports
      q <- positionReports
      if p.VID == q.VID
      if p.Time - q.Time == 30
      if p.Seg != q.Seg && !exitLane(p.Lane)
    } yield p

    val cars = (m: Int, x: Int, s: Int, d: Int) => (for {
      p <- positionReports
      if m == minNumber(p.Time) && p.XWay == x && p.Seg == s && p.Dir == d
    } yield p.VID).distinct


    // TODO: Use windowing (see comment at createWindows)


//    val avgsv = (v: Int, m: Int, x: Int, s: Int, d: Int) => avg(for {
//      p <- positionReports
//      if p.VID == v
//      if m == minNumber(p.Time)
//      if p.XWay == x && p.Seg == s && p.Dir == d
//    } yield p.Spd)
//
//    val avgs = (m: Int, x: Int, s: Int, d: Int) => avg(for {
//      v <- cars(m,x,s,d)
//      a <- avgsv(v,m,x,s,d)
//    } yield a)


    //    val avgs2 = (m: Int, x: Int, s: Int, d: Int) => avg(flatten(
    //      for {
    //        v <- cars(m,x,s,d)
    //      } yield avgsv(v,m,x,s,d)
    //    ))




//    val avgs3 = (m: Int, x: Int, s: Int, d: Int) => avg(for {
//      v <- cars(m,x,s,d)
//    } yield atTime(m*60, avgsv(v,m,x,s,d)))
//
//    val avgs4 = (m: Int, x: Int, s: Int, d: Int) => avg(for {
//      v <- cars(m,x,s,d)
//    } yield avgsv(v,m,x,s,d))
//
//    val avgs5 = (m: Int, x: Int, s: Int, d: Int) => avg(for {
//      v <- cars(m,x,s,d)
//      a <- for { a <- withTimestamp(avgsv(v,m,x,s,d)), a.t == m*60 } yield { a.v }
//      lav <- magicMonadLav
//      if lav < 40
//    } yield a)


//    val triggers: StreamBag[Int] = ???
//    val sum: StreamBag[Double] = ???
//
//    val triggeredSum = for {
//      tri <- triggers
//      s <- sum
//      if (s.t == tri.t)
//    } yield s



  }

  case class Timed()

  def avg(xs: StreamBag[Double]): StreamBag[Double] = ???
  def flatten[A](xss: StreamBag[StreamBag[A]]): StreamBag[A] = ???
  def withTimestamp[A](xss: StreamBag[StreamBag[A]]): StreamBag[A] = ???
}
