# Code/manifest generation for the Kubernetes operator.
CONTROLLER_GEN := go run sigs.k8s.io/controller-tools/cmd/controller-gen

.PHONY: generate manifests test helm-sync-crds helm-lint helm-package e2e-k8s e2e-cloud

# DeepCopy methods for the API types.
generate:
	$(CONTROLLER_GEN) object paths=./internal/api/v1alpha1/...

# CRD + RBAC + webhook manifests.
# The webhook generator produces config/webhook/manifests.yaml from the
# +kubebuilder:webhook marker on KafkaTopicValidator. controller-gen emits
# generic placeholders (webhook-service/system) which are then patched in-place
# to the actual names used in this project, and the cert-manager CA-injection
# annotation is re-added (it would be lost on regen otherwise).
# allowDangerousTypes=true permits float64 in CRD schemas (Kafka quota limits use
# fractional rates); it applies only to the crd generator, not rbac/webhook.
manifests:
	$(CONTROLLER_GEN) crd:allowDangerousTypes=true rbac:roleName=monedula-manager-role paths=./... \
		output:crd:dir=config/crd output:rbac:dir=config/rbac
	$(CONTROLLER_GEN) webhook paths=./... output:webhook:dir=config/webhook
	@# Patch generated webhook manifest: set correct service name, namespace,
	@# webhook config name, and add the cert-manager CA-injection annotation.
	@hack/patch-webhook-annotation.sh config/webhook/manifests.yaml
	$(MAKE) helm-sync-crds

test:
	go test ./...

HELM_CRD_DIR := charts/monedula-gitops/templates/crds
helm-sync-crds:
	@rm -rf $(HELM_CRD_DIR)
	@mkdir -p $(HELM_CRD_DIR)
	@for f in config/crd/gitops.monedula.dev_*.yaml; do \
		base=$$(basename $$f); \
		out=$(HELM_CRD_DIR)/$$base; \
		grep -q '^  annotations:' $$f || { echo "ERROR: $$f has no metadata.annotations block (controller-gen drift)"; exit 1; }; \
		echo '{{- if .Values.crds.enabled }}' > $$out; \
		awk '/^---$$/ && NR==1 {next} \
		     /^  annotations:$$/ && !done { \
		       print; \
		       print "    {{- if .Values.crds.keep }}"; \
		       print "    helm.sh/resource-policy: keep"; \
		       print "    {{- end }}"; \
		       done=1; next \
		     } {print}' $$f >> $$out; \
		echo '{{- end }}' >> $$out; \
	done
	@echo "synced $$(ls $(HELM_CRD_DIR) | wc -l | tr -d ' ') CRDs into the chart"

helm-lint:
	helm lint charts/monedula-gitops

helm-package:
	helm package charts/monedula-gitops --destination dist/

# Opt-in Confluent Cloud validation harness. Never runs in CI; the test's own
# env gating handles credentials (skips without MONEDULA_CLOUD_* vars, fails on
# a half-configured set) — see test/e2e/cloud/README.md for setup.
# -count=1 is required: the harness gates on env vars read via os.Environ(),
# which go test's result cache does not track — a cached "ok" could otherwise
# mask a newly configured run.
e2e-cloud: ## Run the Confluent Cloud validation harness (needs MONEDULA_CLOUD_* env)
	go test -tags cloud ./test/e2e/cloud/ -v -timeout 20m -count=1

e2e-k8s: ## Run the k8s scenario suite over kind (skips if kind/kubectl/bats absent)
	@command -v kind >/dev/null && command -v kubectl >/dev/null && command -v bats >/dev/null \
	  || { echo "skip: kind/kubectl/bats not installed"; exit 0; } && \
	  test/e2e/k8s/setup.sh && \
	  bats test/e2e/k8s/run.bats; \
	  rc=$$?; test/e2e/k8s/teardown.sh; exit $$rc
