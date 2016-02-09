package mesosphere.marathon.api

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Response

import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.plugin.http.HttpResponse

/**
  * Base trait for authentication and authorization in http resource endpoints.
  */
trait AuthResource extends RestResource {
  def authenticator: Authenticator
  def authorizer: Authorizer

  def doIfAuthenticated(request: HttpServletRequest)(fn: Identity => Response): Response = {
    val requestWrapper = new RequestFacade(request)
    val identity = result(authenticator.authenticate(requestWrapper))
    identity.map(fn).getOrElse {
      withResponseFacade(authenticator.handleNotAuthenticated(requestWrapper, _))
    }
  }

  def doIfAuthenticatedAndAuthorized[Resource](request: HttpServletRequest,
                                               action: AuthorizedAction[Resource],
                                               resources: Resource*)(fn: => Response): Response = {
    doIfAuthenticated(request) { identity =>
      doIfAuthorized(action, resources: _*)(fn)(identity)
    }
  }

  def doIfAuthorized[Resource, R >: Resource](
    action: AuthorizedAction[R],
    maybeResource: Option[Resource],
    ifNotExists: Response)(fn: (Resource) => Response)(implicit identity: Identity): Response =
    {
      maybeResource match {
        case Some(resource) => doIfAuthorized(action, resource)(fn(resource))
        case None           => ifNotExists
      }
    }

  def doIfAuthorized[Resource](action: AuthorizedAction[Resource],
                               resources: Resource*)(fn: => Response)(implicit identity: Identity): Response = {
    val areAllActionsAuthorized = resources.forall(authorizer.isAuthorized(identity, action, _))

    if (areAllActionsAuthorized) fn
    else withResponseFacade(authorizer.handleNotAuthorized(identity, _))
  }

  def isAuthorized[T](action: AuthorizedAction[T], resource: T)(implicit identity: Identity): Boolean = {
    authorizer.isAuthorized(identity, action, resource)
  }

  private[this] def withResponseFacade(fn: HttpResponse => Unit): Response = {
    val responseFacade = new ResponseFacade
    fn(responseFacade)
    responseFacade.response
  }
}

