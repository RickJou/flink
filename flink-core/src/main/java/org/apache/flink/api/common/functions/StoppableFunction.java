/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * 必须通过可停止的功能来实现，例如，流工作的源功能。 当作业收到STOP信号时，将调用stop（）方法。 在此信号上，源函数必须停止发送新数据并正常终止。
 * Must be implemented by stoppable functions, eg, source functions of streaming jobs. The method {@link #stop()} will
 * be called when the job received the STOP signal. On this signal, the source function must stop emitting new data and
 * terminate gracefully.
 */
@PublicEvolving
public interface StoppableFunction {
	/**
	 * 停止来源。 与cancel（）相反，这是对源函数正常关闭的请求。 待处理的数据仍然可以发出，并且不需要立即停止 - 但是，在不久的将来。 作业将继续运行，直到完全处理完所有发出的数据。
	 * 大多数流源都会在run（）方法中有一个while循环。 您需要确保源将突破此循环。 这可以通过在循环中检查并在此方法中设置为false的volatile字段“isRunning”来实现。
	 *
	 * Stops the source. In contrast to {@code cancel()} this is a request to the source function to shut down
	 * gracefully. Pending data can still be emitted and it is not required to stop immediately -- however, in the near
	 * future. The job will keep running until all emitted data is processed completely.
	 *
	 * <p>Most streaming sources will have a while loop inside the {@code run()} method. You need to ensure that the source
	 * will break out of this loop. This can be achieved by having a volatile field "isRunning" that is checked in the
	 * loop and that is set to false in this method.
	 *
	 * <p><strong>The call to {@code stop()} should not block and not throw any exception.</strong>
	 */
	void stop();
}
