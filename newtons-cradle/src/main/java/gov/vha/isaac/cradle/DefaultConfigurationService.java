/**
 * Copyright Notice
 * 
 * This is a work of the U.S. Government and is not subject to copyright
 * protection in the United States. Foreign copyrights may apply.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.vha.isaac.cradle;

import gov.vha.isaac.ochre.api.ConfigurationService;
import gov.vha.isaac.ochre.api.LookupService;
import gov.vha.isaac.ochre.api.constants.Constants;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import javax.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.hk2.api.Rank;
import org.jvnet.hk2.annotations.Service;

/**
 * The default implementation of {@link ConfigurationService} which is used to specify where the datastore
 * location is, among other things.
 * 
 * Note that this default implementation has a {@link Rank} of 0. To override this implementation with any
 * other, simply provide another implementation on the classpath with a higher rank.
 * 
 * @author <a href="mailto:daniel.armbrust.list@gmail.com">Dan Armbrust</a>
 */
@Service(name = "Cradle Default Configuration Service")
@Rank(value = 0)
@Singleton
public class DefaultConfigurationService implements ConfigurationService
{
	private Path dataStoreFolderPath_ = null;
	private volatile boolean initComplete_ = false;

	private DefaultConfigurationService()
	{
		//only for HK2
	}

	/**
	 * @see gov.vha.isaac.ochre.api.ConfigurationService#getDataStoreFolderPath()
	 */
	@Override
	public Optional<Path> getDataStoreFolderPath()
	{
		if (dataStoreFolderPath_ == null && !initComplete_)
		{
			synchronized (this)
			{
				if (dataStoreFolderPath_ == null && !initComplete_)
				{
					String dataStoreRootFolder = System.getProperty(Constants.DATA_STORE_ROOT_LOCATION_PROPERTY);
					if (!StringUtils.isBlank(dataStoreRootFolder))
					{
						dataStoreFolderPath_ = Paths.get(dataStoreRootFolder);
						if (!Files.exists(dataStoreFolderPath_))
						{
							try
							{
								Files.createDirectories(dataStoreFolderPath_);
							}
							catch (IOException e)
							{
								throw new RuntimeException("Failure creating dataStoreRootFolder folder: " + dataStoreFolderPath_.toString(), e);
							}
						}

						if (!Files.isDirectory(dataStoreFolderPath_))
						{
							throw new IllegalStateException("The specified path to the db folder appears to be a file, rather than a folder, as expected.  " + " Found: "
									+ dataStoreFolderPath_.toAbsolutePath().toString());
						}
					}
					
					initComplete_ = true;
				}
			}
		}
		return Optional.ofNullable(dataStoreFolderPath_);
	}

	/**
	 * @see gov.vha.isaac.ochre.api.ConfigurationService#setDataStoreFolderPath(java.nio.file.Path)
	 */
	@Override
	public void setDataStoreFolderPath(Path dataStoreFolderPath) throws IllegalStateException, IllegalArgumentException
	{
		if (LookupService.hasIsaacBeenStartedAtLeastOnce())
		{
			throw new IllegalStateException("Can only set the dbFolderPath prior to starting Isaac");
		}
		
		if (Files.exists(dataStoreFolderPath) && !Files.isDirectory(dataStoreFolderPath))
		{
			throw new IllegalArgumentException("The specified path to the db folder appears to be a file, rather than a folder, as expected.  " + " Found: "
					+ dataStoreFolderPath_.toAbsolutePath().toString());
		}
		try
		{
			Files.createDirectories(dataStoreFolderPath);
		}
		catch (IOException e)
		{
			throw new RuntimeException("Failure creating dataStoreFolderPath folder: " + dataStoreFolderPath.toString(), e);
		}

		dataStoreFolderPath_ = dataStoreFolderPath;
	}
}
