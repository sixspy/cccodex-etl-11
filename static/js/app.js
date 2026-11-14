const { createApp } = Vue;

createApp({
  data() {
    return {
      me: "",
      items: [],
      current: null,
      activeTab: "overview",
      tabs: [
        { key: "overview", label: "管道总览" },
        { key: "sources", label: "源对照册" },
        { key: "preview", label: "合并预览" },
        { key: "config", label: "冲突设置" },
      ],
      busy: false,
      msg: "",
      configMsg: "",
      openConflict: "",
      form: { merge_key: "", conflict_policy: "" },
      policyOptions: [
        {
          code: "whole_row_fail",
          name: "整行失败进冲突筐",
          desc: "双源同键不同值时整行判失败，冲突行进入冲突筐等待处理。",
        },
        {
          code: "keep_primary",
          name: "保留主源值并旁注次源差异",
          desc: "采用主源值继续合并，同时在旁注记录次源差异。",
        },
      ],
    };
  },
  computed: {
    isLocked() {
      return !!(this.current && this.current.policy_locked_at);
    },
  },
  async mounted() {
    const me = await fetch("/api/me");
    if (!me.ok) {
      location.href = "/login";
      return;
    }
    this.me = (await me.json()).user;
    await this.refresh();
  },
  methods: {
    statusClass(status) {
      if (status === "已完成") return "ok";
      if (status === "合并中" || status === "已抽取待加载") return "warn";
      return "";
    },
    policyName(code) {
      const match = this.policyOptions.find((o) => o.code === code);
      return match ? match.name : code || "未设置";
    },
    async refresh() {
      const res = await fetch("/api/overview");
      if (res.ok) {
        this.items = (await res.json()).items || [];
      }
      if (this.current) {
        await this.reload(this.current.id);
      }
    },
    async reload(id) {
      const res = await fetch(`/api/pipelines/${id}/merge`);
      if (!res.ok) {
        this.current = null;
        return;
      }
      const data = await res.json();
      const previousTab = this.activeTab;
      this.current = {
        ...data.pipeline,
        merge_status: data.merge_status,
        strategy_name: data.strategy.name,
        policy_locked_code: data.policy_locked_code,
        policy_locked_at: data.policy_locked_at,
        strategy: data.strategy,
        sources: data.sources,
        primary_count: data.overview.primary_count,
        secondary_count: data.overview.secondary_count,
        conflict_count: data.overview.conflict_count,
        preview: data.preview,
        claims: data.claims,
      };
      this.activeTab = previousTab;
      this.resetForm();
    },
    async open(id) {
      this.msg = "";
      this.configMsg = "";
      this.openConflict = "";
      this.activeTab = "overview";
      await this.reload(id);
    },
    resetForm() {
      if (!this.current) return;
      this.form.merge_key = this.current.merge_key || "";
      this.form.conflict_policy =
        this.current.strategy.code || "whole_row_fail";
    },
    toggleConflict(key) {
      this.openConflict = this.openConflict === key ? "" : key;
    },
    async extract() {
      if (!this.current) return;
      this.busy = true;
      this.msg = "";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/extract`, {
          method: "POST",
        });
        const data = await res.json();
        if (!res.ok) {
          this.msg = data.detail || data.error || "抽取失败";
          return;
        }
        this.msg = "两源抽取完成，可继续合并";
        await this.reload(this.current.id);
        await this.refreshList();
      } finally {
        this.busy = false;
      }
    },
    async extractSource(role) {
      if (!this.current) return;
      this.busy = true;
      this.msg = "";
      try {
        const res = await fetch(
          `/api/pipelines/${this.current.id}/sources/${encodeURIComponent(role)}/extract`,
          { method: "POST" }
        );
        const data = await res.json();
        if (!res.ok) {
          this.msg = data.detail || data.error || "抽取失败";
          return;
        }
        this.msg = `${role}抽取完成`;
        await this.reload(this.current.id);
        await this.refreshList();
      } finally {
        this.busy = false;
      }
    },
    async runMerge() {
      if (!this.current) return;
      this.busy = true;
      this.msg = "";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/merge`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            merge_key: this.current.merge_key,
            conflict_policy: this.current.strategy.code,
          }),
        });
        const data = await res.json();
        if (!res.ok) {
          this.msg = data.error || data.detail || "合并失败";
          await this.reload(this.current.id);
          return;
        }
        this.msg = `合并完成，冲突 ${data.conflict_count} 条，合并后 ${data.merged_count} 条`;
        this.activeTab = "preview";
        await this.reload(this.current.id);
        await this.refreshList();
      } finally {
        this.busy = false;
      }
    },
    async saveConfig() {
      if (!this.current) return;
      this.busy = true;
      this.configMsg = "";
      try {
        const res = await fetch(
          `/api/pipelines/${this.current.id}/merge-config`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              merge_key: this.form.merge_key,
              conflict_policy: this.form.conflict_policy,
            }),
          }
        );
        const data = await res.json();
        if (!res.ok) {
          this.configMsg = data.error || data.detail || "保存失败";
          return;
        }
        this.configMsg = "设置已保存";
        await this.reload(this.current.id);
        await this.refreshList();
      } finally {
        this.busy = false;
      }
    },
    async load() {
      if (!this.current) return;
      this.busy = true;
      this.msg = "";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/load`, {
          method: "POST",
        });
        const data = await res.json();
        if (!res.ok) {
          this.msg = data.detail || data.error || "落库失败";
          return;
        }
        this.msg =
          data.status === "已落库" ? "落库完成" : "当前状态不能落库，请先抽取并合并";
        await this.reload(this.current.id);
        await this.refreshList();
      } finally {
        this.busy = false;
      }
    },
    async refreshList() {
      const res = await fetch("/api/overview");
      if (res.ok) {
        this.items = (await res.json()).items || [];
      }
    },
    async logout() {
      await fetch("/api/logout", { method: "POST" });
      location.href = "/login";
    },
  },
}).mount("#app");
