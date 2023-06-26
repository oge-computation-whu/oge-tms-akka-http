package whu.edu.cn

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import geotrellis.layer.SpatialKey
import geotrellis.raster._
import geotrellis.raster.render.Jpg
import geotrellis.store.{AttributeStore, LayerId, Reader, ValueNotFoundError, ValueReader}

import java.net.URI
import scala.concurrent._

object ServeJPG {
  def main(args: Array[String]): Unit = {
    val outputPath = "/mnt/storage/on-the-fly"
    val catalogPath: URI = new java.io.File(outputPath).toURI
    //创建存储区
    val attributeStore: AttributeStore = AttributeStore(catalogPath)
    //创建valuereader，用来读取每个tile的value
    val valueReader: ValueReader[LayerId] = ValueReader(attributeStore, catalogPath)

    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val route: Route = cors() {
      pathPrefix(Segment) {
        layerId =>
          pathPrefix(IntNumber) {
            zoom =>
              // ZXY route:
              pathPrefix(IntNumber / IntNumber) { (x, y) =>
                complete {
                  Future {
                    // Read in the tile at the given z/x/y coordinates.
                    val tileOpt: Option[MultibandTile] =
                      try {
                        val reader: Reader[SpatialKey, MultibandTile] = valueReader.reader[SpatialKey, MultibandTile](LayerId(layerId, zoom))
                        Some(reader.read(x, y))
                      } catch {
                        case _: ValueNotFoundError =>
                          None
                      }
                    for (tile <- tileOpt) yield {
                      val product: MultibandTile = rasterFunction(tile)
                      var jpg: Jpg = null
                      val bandCount: Int = product.bandCount
                      if (bandCount == 1) {
                        jpg = product.band(0).renderJpg()
                      }
                      else if (bandCount == 3) {
                        jpg = MultibandTile(product.bands.take(3)).renderJpg()
                      }
                      else {
                        throw new RuntimeException("波段数量不是1或3，无法渲染！")
                      }
                      pngAsHttpResponse(jpg)
                    }
                  }
                }
              }
          }
      } ~
        // Static content routes:
        pathEndOrSingleSlash {
          getFromFile("static/index.html")
        } ~
        pathPrefix("") {
          getFromDirectory("static")
        }
    }
    val binding: Future[Http.ServerBinding] = Http().bindAndHandle(route, "0.0.0.0", 19101)
  }

  /** raster transformation to perform at request time */
  def rasterFunction(multibandTile: MultibandTile): MultibandTile = {
    multibandTile.convert(DoubleConstantNoDataCellType)
  }

  def pngAsHttpResponse(jpg: Jpg): HttpResponse =
    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), jpg.bytes))
}
