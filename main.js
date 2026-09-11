// JunifyDB site: tabs, copy buttons, mobile nav. No dependencies.
(function () {
  "use strict";

  // Tabs
  var tabs = document.querySelectorAll(".tab");
  var panels = document.querySelectorAll(".tab-panel");
  tabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      tabs.forEach(function (t) {
        t.classList.remove("active");
        t.setAttribute("aria-selected", "false");
      });
      panels.forEach(function (p) { p.classList.remove("active"); });
      tab.classList.add("active");
      tab.setAttribute("aria-selected", "true");
      var panel = document.getElementById("panel-" + tab.dataset.tab);
      if (panel) panel.classList.add("active");
    });
  });

  // Copy buttons (decode HTML entities back to source text)
  var snippets = {
    maven: '<dependency>\n    <groupId>org.junify.db</groupId>\n    <artifactId>junify-db-core</artifactId>\n    <version>1.0.0</version>\n</dependency>',
    gradle: 'implementation("org.junify.db:junify-db-core:1.0.0")',
    java: 'var db = JunifyDB.embed()\n    .storageEngine("FILE")   // or IN_MEMORY, B_TREE, LSM_TREE\n    .persistTo("data")\n    .build();\n\nvar users = db.documentCollection("users");\nvar user = new Document();\nuser.id("user-1");\nuser.add("name", "Alice");\nuser.add("age", 30);\nusers.insert(user);\n\nvar adults = users.find(Query.eq("age", 30));',
    rest: '# Start the server\njava -jar junify-db-core.jar --port 8080 --engine FILE --data-dir ./data\n\n# Health check\ncurl http://localhost:8080/api/health\n\n# Create a document\ncurl -X POST http://localhost:8080/api/collections/users \\\n    -H "Content-Type: application/json" \\\n    -d \'{"name": "Alice", "email": "alice@example.com"}\''
  };
  document.querySelectorAll(".copy").forEach(function (btn) {
    btn.addEventListener("click", function () {
      var text = snippets[btn.dataset.copy] || "";
      var done = function () {
        btn.textContent = "Copied";
        setTimeout(function () { btn.textContent = "Copy"; }, 1600);
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(done, done);
      } else {
        var ta = document.createElement("textarea");
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        try { document.execCommand("copy"); } catch (e) { /* noop */ }
        document.body.removeChild(ta);
        done();
      }
    });
  });

  // Mobile nav
  var toggle = document.querySelector(".nav-toggle");
  var links = document.querySelector(".nav-links");
  if (toggle && links) {
    toggle.addEventListener("click", function () {
      var open = links.classList.toggle("open");
      toggle.setAttribute("aria-expanded", open ? "true" : "false");
    });
    links.querySelectorAll("a").forEach(function (a) {
      a.addEventListener("click", function () {
        links.classList.remove("open");
        toggle.setAttribute("aria-expanded", "false");
      });
    });
  }
})();
