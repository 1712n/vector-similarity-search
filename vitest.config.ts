import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";
import { Response } from "miniflare";

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: () => {
        process.env["WRANGLER_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE"] =
          "postgres://user:password@localhost:5432/databasename";

        return {
          singleWorker: true,
          wrangler: { configPath: "./wrangler.toml" },
          miniflare: {
            // Required to use `SELF.scheduled()`. This is an experimental
            // compatibility flag, and cannot be enabled in production.
            compatibilityFlags: ["nodejs_compat", "service_binding_extra_handlers"],
            wrappedBindings: {
              AI: {
                scriptName: "workers-ai"
              },
            },
            workers: [
              {
                name: "workers-ai",
                modules: true,
                script: `export default function() {
                  return {
                    run: async (model, data) => {
                      return {
                        shape: [data.text.length, 768],
                        data: data.text.map(() => 
                          Array.from({length: 768}, () => 0.03190500661730766)
                        )
                      };
                    }
                  };
                };`
              },
            ],
          },
        };
      },
    },
  },
});
