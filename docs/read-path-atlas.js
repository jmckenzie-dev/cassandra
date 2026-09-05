/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

(() => {
  "use strict";

  document.documentElement.classList.add("js");

  const progress = document.querySelector(".reading-progress span");
  const updateProgress = () => {
    if (!progress) return;
    const scrollable = document.documentElement.scrollHeight - window.innerHeight;
    const value = scrollable > 0 ? (window.scrollY / scrollable) * 100 : 0;
    progress.style.width = `${Math.min(100, Math.max(0, value))}%`;
  };

  addEventListener("scroll", updateProgress, { passive: true });
  updateProgress();

  const links = [...document.querySelectorAll(".toc-nav a[data-section]")];
  const sections = [...document.querySelectorAll("main section[id]")];
  if ("IntersectionObserver" in window && links.length > 0) {
    const activeObserver = new IntersectionObserver((entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((left, right) => right.intersectionRatio - left.intersectionRatio)[0];
      if (!visible) return;
      links.forEach((link) => {
        link.classList.toggle("active", link.dataset.section === visible.target.id);
      });
    }, { rootMargin: "-18% 0px -62% 0px", threshold: [0, 0.2, 0.5] });

    sections.forEach((section) => activeObserver.observe(section));
  }

  const reveals = [...document.querySelectorAll(".reveal")];
  if ("IntersectionObserver" in window) {
    const revealObserver = new IntersectionObserver((entries, observer) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add("visible");
        observer.unobserve(entry.target);
      });
    }, { threshold: 0.07 });

    reveals.forEach((element, index) => {
      element.style.transitionDelay = `${Math.min(index % 4, 3) * 45}ms`;
      revealObserver.observe(element);
    });
  } else {
    reveals.forEach((element) => element.classList.add("visible"));
  }
})();
