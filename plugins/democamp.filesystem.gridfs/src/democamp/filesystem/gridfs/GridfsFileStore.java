package democamp.filesystem.gridfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Set;
import java.util.regex.Pattern;

import org.eclipse.core.filesystem.EFS;
import org.eclipse.core.filesystem.IFileInfo;
import org.eclipse.core.filesystem.IFileStore;
import org.eclipse.core.filesystem.IFileSystem;
import org.eclipse.core.filesystem.provider.FileInfo;
import org.eclipse.core.filesystem.provider.FileStore;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.Status;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class GridfsFileStore extends FileStore {

	private static final byte[] EMPTY_DATA = new byte[0];
	private static final String DB_NAME = "gridfsdb";
	
	private final Mongo mongo;
	private final GridFS gridfs;
	private final IPath path;

	public GridfsFileStore(Mongo mongo, IPath path) {
		this.mongo = mongo;
		this.gridfs = new GridFS(mongo.getDB(DB_NAME));
		this.path = path;
	}

	@Override
	public String[] childNames(int options, IProgressMonitor monitor) throws CoreException {
		GridFSDBFile file = getGridFSDBFile();
		
		if (file == null) {
			return EMPTY_STRING_ARRAY;
		}
		
		if (file.containsField("isDirectory") && (Boolean) file.get("isDirectory")) {
			try {
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				file.writeTo(outputStream);
				
				String contents = outputStream.toString("UTF-8");
				
				Iterable<String> files = Splitter.on(Pattern.compile("\r?\n")).omitEmptyStrings().split(contents);
				return Iterables.toArray(files, String.class);
				
			} catch (IOException e) {
				throw new CoreException(new Status(Status.ERROR, Activator.PLUGIN_ID, 
						MessageFormat.format("Exception occurred while reading directory contents of ''{0}''.", path), e)); 
			}
		}
		
		return EMPTY_STRING_ARRAY;
	}

	@Override
	public IFileStore getChild(String name) {
		return new GridfsFileStore(mongo, path.append(name));
	}

	@Override
	public String getName() {
		String lastSegment = path.lastSegment();
		return (lastSegment == null) ? "" : lastSegment;
	}

	@Override
	public IFileStore getParent() {
		if (path.isRoot()) {
			return null;
		} else {
			return new GridfsFileStore(mongo, getParentPath());
		}
	}

	private IPath getParentPath() {
		return path.removeLastSegments(1);
	}

	@Override
	public IFileSystem getFileSystem() {
		return GridfsFileSystem.getInstance();
	}

	@Override
	public URI toURI() {
		try {
			ServerAddress address = mongo.getAddress();
			String host = address.getHost();
			int port = address.getPort();
			
			return new URI(
					getFileSystem().getScheme(), 
					null, 
					host, 
					port, 
					path.toString(), 
					null, 
					null);
		} catch (URISyntaxException e) {
			throw new IllegalStateException(
					MessageFormat.format("Invalid path ''{0}''.", path), e);
		}
	}

	@Override
	public IFileInfo fetchInfo(int options, IProgressMonitor monitor) throws CoreException {
		GridFSDBFile file = getGridFSDBFile();
		FileInfo fi = new FileInfo(getName());
		
		if (file == null) {
			if (getName().isEmpty()) {
				// Create root directory entry and try again
				GridFSInputFile inputFile = gridfs.createFile(EMPTY_DATA);
				inputFile.setFilename(path.toString());
				inputFile.getMetaData().put("isDirectory", true);
				inputFile.save();
				
				file = getGridFSDBFile();
			} else {
				fi.setExists(false);
				return fi;
			}
		}
		
		fi.setExists(true);
		fi.setLength(file.getLength());
	
		if (file.containsField("isDirectory") && (Boolean) file.get("isDirectory")) {
			fi.setDirectory(true);
		}
		
		return fi;
	}

	@Override
	public InputStream openInputStream(int options, IProgressMonitor monitor) throws CoreException {
		GridFSDBFile file = getGridFSDBFile();
		
		if (file == null) {
			throw new CoreException(new Status(Status.ERROR, Activator.PLUGIN_ID, 
					MessageFormat.format("Path ''{0}'' does not exist.", path)));
		}
		
		if (file.containsField("isDirectory") && (Boolean) file.get("isDirectory")) {
			throw new CoreException(new Status(Status.ERROR, Activator.PLUGIN_ID, 
					MessageFormat.format("Path ''{0}'' is a directory.", path)));
		}
		
		return file.getInputStream();
	}
	
	@Override
	public IFileStore mkdir(int options, IProgressMonitor monitor) throws CoreException {
		GridFSDBFile file = getGridFSDBFile();
		
		if (file != null) {
			if (file.containsField("isDirectory") && (Boolean) file.get("isDirectory")) {
				return this;
			}
			
			throw new CoreException(new Status(Status.ERROR, Activator.PLUGIN_ID, 
					MessageFormat.format("Couldn't create directory ''{0}''; a file with this path already exists.", path)));
		}
		
		IFileStore parent = getParent();
		
		if (parent != null) {
			IFileInfo parentInfo = parent.fetchInfo();
			
			if (!parentInfo.isDirectory()) {
				throw new CoreException(new Status(Status.ERROR, Activator.PLUGIN_ID, 
						MessageFormat.format("Couldn't create directory ''{0}''; path parent is not a directory.", path)));
			}
	
			if (!parentInfo.exists()) {
				
				if ((EFS.SHALLOW & options) != 0) {
					throw new CoreException(new Status(Status.ERROR, Activator.PLUGIN_ID, 
							MessageFormat.format("Couldn't create directory ''{0}''; parent directory does not exist.", path)));
				} else {
					parent.mkdir(options, null);
				}
			}
			
			addToParentNames(getName());
		}
			
		if (file == null) {
			GridFSInputFile newDirectory = gridfs.createFile(EMPTY_DATA);
			newDirectory.setFilename(path.toString());
			newDirectory.getMetaData().put("isDirectory", true);
			newDirectory.save();
		}
		
		return this;
	}

	private void addToParentNames(String name) throws CoreException {
		Set<String> parentNames = Sets.newHashSet(getParent().childNames(EFS.NONE, null));
		
		if (parentNames.add(name)) {
			updateParentNames(parentNames);
		}
	}

	private void removeFromParentNames(String name) throws CoreException {
		Set<String> parentNames = Sets.newHashSet(getParent().childNames(EFS.NONE, null));
		
		if (parentNames.remove(name)) {
			updateParentNames(parentNames);
		}
	}	

	private void updateParentNames(Set<String> parentNames) {
		gridfs.remove(getParentPath().toString());

		GridFSInputFile parentFile = gridfs.createFile(getParentPath().toString());
		parentFile.getMetaData().put("isDirectory", true);
		PrintWriter parentFileWriter = new PrintWriter(parentFile.getOutputStream());
		
		for (String parentName : parentNames) {
			parentFileWriter.println(parentName);
		}
		
		// Hides IOException from Mongo's output stream
		parentFileWriter.close();
	}
	
	@Override
	public void putInfo(IFileInfo info, int options, IProgressMonitor monitor) throws CoreException {
		// NOP
	}
	
	@Override
	public OutputStream openOutputStream(int options, IProgressMonitor monitor) throws CoreException {
		GridFSDBFile file = getGridFSDBFile();
		
		if (file != null) {
			if (file.containsField("isDirectory") && (Boolean) file.get("isDirectory")) {
				throw new CoreException(new Status(Status.ERROR, Activator.PLUGIN_ID, 
						MessageFormat.format("Couldn't create output stream for ''{0}''; this path represents a directory.", path)));
			}
		}
		
		IFileStore parent = getParent();
		IFileInfo parentInfo = parent.fetchInfo();
		
		if (!parentInfo.exists()) {
			throw new CoreException(new Status(Status.ERROR, Activator.PLUGIN_ID, 
					MessageFormat.format("Couldn't create output stream for ''{0}''; parent does not exist.", path)));
		}

		gridfs.remove(path.toString());
		addToParentNames(getName());
		
		GridFSInputFile inputFile = gridfs.createFile(path.toString());
		return inputFile.getOutputStream();
	}

	@Override
	public void delete(int options, IProgressMonitor monitor) throws CoreException {
		removeFromParentNames(getName());
		gridfs.remove(path.toString());
	}
	
	private GridFSDBFile getGridFSDBFile() {
		return gridfs.findOne(path.toString());
	}
}
