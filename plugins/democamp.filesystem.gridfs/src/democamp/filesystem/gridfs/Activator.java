package democamp.filesystem.gridfs;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.eclipse.core.runtime.ILog;
import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.Status;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import com.google.common.collect.Maps;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;

public class Activator implements BundleActivator {

	public static final String PLUGIN_ID = "com.b2international.democamp.filesystem.gridfs";
	
	private static Activator instance;
	private static ILog log;

	static Activator getInstance() {
		return instance;
	}

	private Map<ServerAddress, Mongo> mongoConnections;
	private final Object mongoConnectionsLock = new Object();
	
	public Mongo getMongo(String host, int port) throws UnknownHostException {
		
		ServerAddress address = new ServerAddress(host, port);
		Mongo mongo = new Mongo(address);
		
		synchronized (mongoConnectionsLock) {
			
			if (!mongoConnections.containsKey(address)) {
				mongoConnections.put(address, mongo);
				return mongo;
			} else {
				return mongoConnections.get(address);
			}
		}
	}
	
	@Override
	public void start(BundleContext bundleContext) throws Exception {
		mongoConnections = Maps.newHashMap();
		
		log = Platform.getLog(bundleContext.getBundle());
		instance = this;
	}

	@Override
	public void stop(BundleContext bundleContext) throws Exception {
		instance = null;
		log = null;
		
		synchronized (mongoConnectionsLock) {
			Collection<Mongo> values = mongoConnections.values();

			for (Iterator<Mongo> itr = values.iterator(); itr.hasNext();) {
				itr.next().close();
				itr.remove();
			}
		}
		
		mongoConnections = null;
	}

	public void logError(String message) {
		log.log(new Status(Status.ERROR, PLUGIN_ID, message));
	}
	
	public void logError(String message, Throwable t) {
		log.log(new Status(Status.ERROR, PLUGIN_ID, message, t));
	}
}
