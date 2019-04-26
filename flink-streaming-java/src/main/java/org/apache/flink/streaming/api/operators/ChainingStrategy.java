/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * 定义运算符的链接策略。
 * 当操作符链接到前一个操作符时，意味着它们在同一个线程中运行。 它们成为一个由多个步骤组成的运算符。
 * StreamOperator使用的默认值是HEAD，这意味着运算符未链接到其前一个。 大多数操作员使用ALWAYS覆盖它，这意味着它们将尽可能地链接到前面的操作符。
 *
 * Defines the chaining scheme for the operator. When an operator is chained to the
 * predecessor, it means that they run in the same thread. They become one operator
 * consisting of multiple steps.
 *
 * <p>The default value used by the {@link StreamOperator} is {@link #HEAD}, which means that
 * the operator is not chained to its predecessor. Most operators override this with
 * {@link #ALWAYS}, meaning they will be chained to predecessors whenever possible.
 */
@PublicEvolving
public enum ChainingStrategy {

	/**
	 * 只要有可能，尽可能的链接操作符。
	 * Operators will be eagerly chained whenever possible.
	 *
	 * 为了优化性能，通常允许最大链接并增加操作员并行性是一种好习惯。
	 * <p>To optimize performance, it is generally a good practice to allow maximal
	 * chaining and increase operator parallelism.
	 */
	ALWAYS,

	/**
	 * 操作符不会被链接到前面或后面的操作符。
	 * The operator will not be chained to the preceding or succeeding operators.
	 */
	NEVER,

	/**
	 * 对于SourceOperator来说符合使用场景
	 * 操作符不会链接到前面的操作符,但是后面的操作符可能会连接到该操作符
	 * The operator will not be chained to the predecessor, but successors may chain to this
	 * operator.
	 */
	HEAD
}
