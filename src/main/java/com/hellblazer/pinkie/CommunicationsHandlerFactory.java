/*
 * Copyright (c) 2009, 2011 Hal Hildebrand, all rights reserved.
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
package com.hellblazer.pinkie;

import java.nio.channels.SocketChannel;

/**
 * Creates instances of communications handlers
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public interface CommunicationsHandlerFactory<T extends CommunicationsHandler> {
    /**
     * Create a new instance of a communications handler
     * 
     * @param channel
     *            - the inbound socket channel
     * 
     * @return the new instance
     */
    T createCommunicationsHandler(SocketChannel channel);
}
