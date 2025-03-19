import com.niit.Pro.{BrandData, BrandPercentage, TopBrandsInShandong, TopProvince}
import lxa.code.{RFM, TransVolByHour, UserConversion, WeekArpu}
import main.scala.lxa.code.BrandTop5
import main.scala.zzq.coding.Frau_ECom

object SparkApp {
  def main(args: Array[String]): Unit = {

    BrandTop5.main()
    RFM.main()
    TransVolByHour.main()
    WeekArpu.main()
    UserConversion.main()

    Frau_ECom.main()

    BrandData.main()
    BrandPercentage.main()
    TopBrandsInShandong.main()
    TopProvince.main()
  }
}
