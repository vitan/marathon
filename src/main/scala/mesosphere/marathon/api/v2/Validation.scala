package mesosphere.marathon.api.v2

import java.net._

import com.wix.accord._
import mesosphere.marathon.{ AllConf, ValidationFailedException }
import mesosphere.marathon.state.FetchUri
import play.api.libs.json._

import scala.reflect.ClassTag
import scala.util.Try

object Validation {

  def validate[T](t: T)(implicit validator: Validator[T]): Result = validator.apply(t)
  def validateOrThrow[T](t: T)(implicit validator: Validator[T]): T = validate(t) match {
    case Success    => t
    case f: Failure => throw new ValidationFailedException(t, f)
  }

  implicit def optional[T](implicit validator: Validator[T]): Validator[Option[T]] = {
    new Validator[Option[T]] {
      override def apply(option: Option[T]): Result = option.map(validator).getOrElse(Success)
    }
  }

  implicit def every[T](implicit validator: Validator[T]): Validator[Iterable[T]] = {
    new Validator[Iterable[T]] {
      override def apply(seq: Iterable[T]): Result = {

        val violations = seq.map(item => (item, validator(item))).zipWithIndex.collect {
          case ((item, f: Failure), pos: Int) => GroupViolation(item, "not valid", Some(s"[$pos]"), f.violations)
        }

        if (violations.isEmpty) Success
        else Failure(Set(GroupViolation(seq, "Seq contains elements, which are not valid.", None, violations.toSet)))
      }
    }
  }

  implicit lazy val failureWrites: Writes[Failure] = Writes { f =>
    Json.obj("message" -> "Object is not valid",
      "details" -> f.violations.flatMap(allRuleViolationsWithFullDescription(_)))
  }

  implicit lazy val ruleViolationWrites: Writes[RuleViolation] = Writes { v =>
    Json.obj(
      "attribute" -> v.description,
      "error" -> v.constraint
    )
  }

  private def allRuleViolationsWithFullDescription(violation: Violation,
                                                   parentDesc: Option[String] = None): Set[RuleViolation] = {
    def concatPath(parent: String, child: Option[String], attachDot: Boolean): String = {
      child.map(c => parent + c + { if (attachDot) "." else "" }).getOrElse(parent)
    }

    violation match {
      case r: RuleViolation => Set(parentDesc.map(p =>
        r.withDescription(concatPath(p, r.description, attachDot = false)))
        .getOrElse(r))
      case g: GroupViolation => g.children.flatMap { c =>
        val desc = parentDesc.map { p =>
          val attachDot = g.value match {
            case _: Iterable[_] => false
            case _              => true
          }
          Some(concatPath(p, g.description, attachDot))
        } getOrElse {
          g.description.map(d => concatPath("", Some(d), attachDot = true))
        }
        allRuleViolationsWithFullDescription(c, desc)
      }
    }
  }

  def urlCanBeResolvedValidator: Validator[String] = {
    new Validator[String] {
      def apply(url: String) = {
        Try {
          new URL(url).openConnection() match {
            case http: HttpURLConnection =>
              http.setRequestMethod("HEAD")
              if (http.getResponseCode == HttpURLConnection.HTTP_OK) Success
              else Failure(Set(RuleViolation(url, "URL could not be resolved.", None)))
            case other: URLConnection =>
              other.getInputStream
              Success //if we come here, we could read the stream
          }
        }.getOrElse(
          Failure(Set(RuleViolation(url, "URL could not be resolved.", None)))
        )
      }
    }
  }

  def fetchUriIsValid: Validator[FetchUri] = {
    new Validator[FetchUri] {
      def apply(uri: FetchUri) = {
        try {
          new URI(uri.uri)
          Success
        }
        catch {
          case _: URISyntaxException => Failure(Set(RuleViolation(uri.uri, "URI has invalid syntax.", None)))
        }
      }
    }
  }

  def elementsAreUnique[A](p: Seq[A] => Seq[A] = { seq: Seq[A] => seq },
                           errorMessage: String = "Elements must be unique."): Validator[Seq[A]] = {
    new Validator[Seq[A]] {
      def apply(seq: Seq[A]) = {
        val filteredSeq = p(seq)
        if (filteredSeq.size == filteredSeq.distinct.size) Success
        else Failure(Set(RuleViolation(seq, errorMessage, None)))
      }
    }
  }

  def theOnlyDefinedOptionIn[A <: Product: ClassTag, B](product: A): Validator[Option[B]] =
    new Validator[Option[B]] {
      def apply(option: Option[B]) = {
        option match {
          case Some(prop) =>
            val n = product.productIterator.count {
              case Some(_) => true
              case _       => false
            }

            if (n == 1)
              Success
            else
              Failure(Set(RuleViolation(product, s"not allowed in conjunction with other properties.", None)))
          case None => Success
        }
      }
    }

  def oneOf[T <: AnyRef](options: Set[T]): Validator[T] = {
    import ViolationBuilder._
    new NullSafeValidator[T](
      test = options.contains,
      failure = _ -> s"is not one of (${options.mkString(",")})"
    )
  }

  def oneOf[T <: AnyRef](options: T*): Validator[T] = {
    import ViolationBuilder._
    new NullSafeValidator[T](
      test = options.contains,
      failure = _ -> s"is not one of (${options.mkString(",")})"
    )
  }

  def configValueSet[T <: AnyRef](config: String*): Validator[T] = {
    new Validator[T] {
      override def apply(t: T): Result = {
        if (config.forall(AllConf.suppliedOptionNames)) Success else Failure(Set(RuleViolation(t,
          s"""You have to supply ${config.mkString(", ")} on the command line.""", None)))
      }
    }
  }
}
