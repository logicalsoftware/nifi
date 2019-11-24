/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.*;

@EventDriven
@SideEffectFree
@Tags({"extract", "text", "split", "header", "remove"})
@CapabilityDescription(
        "Accepts arbitrary text, removes the configured number of lines from the beginning of the file and routes the "
                + "rest to the body relationship.  The removed lines are routed to the removed relationship."
)
public class ExtractHeader extends AbstractProcessor {
    public static final PropertyDescriptor REMOVE_NUM_LINES = new PropertyDescriptor.Builder()
            .name("Remove Number of Lines")
            .description("The number of lines to remove from the file")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor KEEP_TRAILING_NEWLINE = new PropertyDescriptor.Builder()
            .name("Keep Trailing Newlines")
            .description("Whether to keep the final newline in the resulting body")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    public static final Relationship REMOVED = new Relationship.Builder()
            .name("removed")
            .description("A new FlowFile containing the removed lines")
            .build();

    public static final Relationship BODY = new Relationship.Builder()
            .name("body")
            .description("A new FlowFile containing the body of the file, minus the removed lines")
            .build();

    public static final Relationship ERROR = new Relationship.Builder()
            .name("error")
            .description("FlowFiles that cause errors during processing")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private int numLinesToRemove;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(BODY);
        rels.add(REMOVED);
        rels.add(ERROR);
        this.relationships = Collections.unmodifiableSet(rels);

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(REMOVE_NUM_LINES);
        this.properties = Collections.unmodifiableList(props);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        numLinesToRemove = context.getProperty(REMOVE_NUM_LINES).asInteger();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        session.transfer(flowFile, BODY);
    }
}
