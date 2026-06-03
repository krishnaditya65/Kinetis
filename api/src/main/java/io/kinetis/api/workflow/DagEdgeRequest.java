package io.kinetis.api.workflow;

import jakarta.validation.constraints.NotBlank;

/** A directed edge: node {@code from} must succeed before node {@code to} starts. */
public record DagEdgeRequest(@NotBlank String from, @NotBlank String to) {}
