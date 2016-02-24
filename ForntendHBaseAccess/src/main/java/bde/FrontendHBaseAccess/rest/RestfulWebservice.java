package bde.FrontendHBaseAccess.rest;

import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.data.Protocol;
import org.restlet.routing.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bde.FrontendHBaseAccess.HBaseManager;
import bde.FrontendHBaseAccess.rest.ressources.GetScoreRessource;

public class RestfulWebservice extends Application {

	private HBaseManager hbm = null;
	final Logger logger = LoggerFactory.getLogger(RestfulWebservice.class);

	public RestfulWebservice() throws Exception {

		Context context = new Context();
		context.getParameters().add("socketTimeout", "1000000000");
		context.getParameters().add("maxIdleTimeout", "1000000000");
		context.getParameters().add("idleTimeout", "1000000000");
		context.getParameters().add("connectionTimeout", "1000000000");
		context.getParameters().add("maxIoIdleTimeMs", "0");
		context.getParameters().add("ioMaxIdleTimeMs", "0");
		context.getParameters().add("readTimeout", "1000000000");

		Component component = new Component();
		component.getServers().add(Protocol.HTTP, 9999);
		component.getServers().setContext(context);
		component.getDefaultHost().attach("/HBase", this);
		component.start();

		this.hbm = HBaseManager.getInstance();
	}

	@Override
	public synchronized Restlet createInboundRoot() {

		// Restlet Router: Leitet Aufrufe an Klassen weiter
		Router router = new Router(getContext());

		if (this.hbm == null) {
			this.hbm = HBaseManager.getInstance();
			router.getContext().getAttributes().put("HBaseManager", this.hbm);
		} else {
			router.getContext().getAttributes().put("HBaseManager", this.hbm);
		}

		// Definition der Routen
		router.attach("/getScore", GetScoreRessource.class);

		return router;
	}
}
