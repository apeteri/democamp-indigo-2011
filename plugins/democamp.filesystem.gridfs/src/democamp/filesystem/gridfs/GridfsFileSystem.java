package democamp.filesystem.gridfs;

import java.net.URI;
import java.net.UnknownHostException;

import org.eclipse.core.filesystem.EFS;
import org.eclipse.core.filesystem.IFileStore;
import org.eclipse.core.filesystem.IFileSystem;
import org.eclipse.core.filesystem.provider.FileSystem;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.Path;

import com.mongodb.Mongo;
import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;

public class GridfsFileSystem extends FileSystem {

	private static IFileSystem instance;
	
	public static IFileSystem getInstance() {
		return instance;
	}
	
	public GridfsFileSystem() {
		super();
		instance = this;
	}

	@Override
	public boolean canDelete() {
		return true;
	}
	
	@Override
	public boolean canWrite() {
		return true;
	}
	
	@Override
	public IFileStore getStore(URI uri) {
		
		try {
			
			String host = uri.getHost();
			if (host == null || host.isEmpty()) host = ServerAddress.defaultHost();
			
			int port = uri.getPort();
			if (port == -1) port = ServerAddress.defaultPort();

			Mongo mongo = Activator.getInstance().getMongo(host, port);
			
			String path = uri.getPath();
			if (path == null) path = "";
			IPath absolutePath = new Path(path).makeAbsolute().removeTrailingSeparator();
			
			return new GridfsFileStore(mongo, absolutePath);
			
		} catch (UnknownHostException e) {
			return EFS.getNullFileSystem().getStore(uri);
		} catch (MongoInternalException e) {
			return EFS.getNullFileSystem().getStore(uri);
		}
	}
}
