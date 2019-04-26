/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamSource;

import java.util.Collection;
import java.util.Collections;

/**
 * 这代表了一个Source。 这实际上并没有转换任何东西，因为它没有输入，但它是任何拓扑的根
 * This represents a Source. This does not actually transform anything since it has no inputs but
 * it is the root {@code StreamTransformation} of any topology.
 *
 * @param <T> The type of the elements that this source produces
 */
@Internal
public class SourceTransformation<T> extends StreamTransformation<T> {

	private final StreamSource<T, ?> operator;

	/**
	 * 创建一个新的sourceTransformation
	 * Creates a new {@code SourceTransformation} from the given operator.
	 *
	 * @param name The name of the {@code SourceTransformation}, this will be shown in Visualizations and the Log
	 * @param operator The {@code StreamSource} that is the operator of this Transformation
	 * @param outputType The type of the elements produced by this {@code SourceTransformation}
	 * @param parallelism The parallelism of this {@code SourceTransformation}
	 */
	public SourceTransformation(
			String name,
			StreamSource<T, ?> operator,
			TypeInformation<T> outputType,
			int parallelism) {
		//实际上是创建了一个streamTransformation
		super(name, outputType, parallelism);
		//绑定source操作符
		this.operator = operator;
	}

	/**
	 * Returns the {@code StreamSource}, the operator of this {@code SourceTransformation}.
	 */
	public StreamSource<T, ?> getOperator() {
		return operator;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		return Collections.<StreamTransformation<?>>singleton(this);
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operator.setChainingStrategy(strategy);
	}
}
