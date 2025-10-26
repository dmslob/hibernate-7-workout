package com.dmslob;

import com.dmslob.entity.Contact;
import jakarta.persistence.metamodel.SingularAttribute;
import jakarta.persistence.metamodel.StaticMetamodel;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Order;
import org.hibernate.query.Page;
import org.hibernate.query.Query;
import org.hibernate.query.restriction.Restriction;
import org.hibernate.query.specification.MutationSpecification;
import org.hibernate.query.specification.SelectionSpecification;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class HibernateTest {

    SessionFactory sessionFactory() {
        return new Configuration()
                .configure("hibernate.cfg.xml")
                .buildSessionFactory();
    }

    List<Contact> findMultiple(SessionFactory sessionFactory) {
        try (Session session = sessionFactory.openSession()) {
            return session.findMultiple(
                    Contact.class,
                    List.of(7, 8, 9),
                    LockMode.OPTIMISTIC);
        }
    }

    @Test
    public void should_use_findMultiple_to_get_contacts() {
        // given
        try (SessionFactory sessionFactory = sessionFactory()) {
            // when
            List<Contact> contacts = findMultiple(sessionFactory);
            // then
            assertThat(contacts).isNotNull();
            assertThat(contacts.size()).isEqualTo(3);
        }
    }

    List<Contact> queryList(SessionFactory sessionFactory) {
        try (Session session = sessionFactory.openSession()) {
            Query<Contact> query = session.createQuery("from Contact", Contact.class);
            return query.list();
        }
    }

    @Test
    public void should_use_queryList_to_get_contacts() {
        // given
        try (SessionFactory sessionFactory = sessionFactory()) {
            // when
            List<Contact> contacts = queryList(sessionFactory);
            // then
            assertThat(contacts).isNotNull();
            assertThat(contacts.size()).isEqualTo(2);
        }
    }

    Contact find(SessionFactory sessionFactory) {
        try (Session session = sessionFactory.openSession()) {
            return session.find(Contact.class, 8, LockMode.OPTIMISTIC);
        }
    }

    @Test
    public void should_use_find_to_get_contact() {
        // given
        try (SessionFactory sessionFactory = sessionFactory()) {
            // when
            Contact contact = find(sessionFactory);
            // then
            assertThat(contact).isNotNull();
            assertThat(contact.getId()).isEqualTo(8);
        }
    }

    List<Contact> getMultiple(SessionFactory sessionFactory) {
        try (StatelessSession session = sessionFactory.openStatelessSession()) {
            return session.getMultiple(
                    Contact.class,
                    List.of(7, 8, 9),
                    LockMode.OPTIMISTIC);
        }
    }

    @Test
    public void should_use_getMultiple_to_get_contacts() {
        // given
        try (SessionFactory sessionFactory = sessionFactory()) {
            // when
            var contacts = getMultiple(sessionFactory);
            // then
            assertThat(contacts).isNotNull();
            assertThat(contacts.size()).isEqualTo(3);
        }
    }

    List<Integer> generateSeries(SessionFactory sessionFactory) {
        try (Session session = sessionFactory.openSession()) {
            String query = "SELECT g.series FROM generate_series(1,5) AS g(series)";
            return session.createNativeQuery(query, Integer.class)
                    .getResultList();
        }
    }

    @Test
    public void should_use_generate_series() {
        // given
        try (SessionFactory sessionFactory = sessionFactory()) {
            // when
            var series = generateSeries(sessionFactory);
            // then
            assertThat(series).isNotNull();
            assertThat(series.size()).isEqualTo(5);
        }
    }

    void create(SessionFactory sessionFactory, Contact newContact) {
        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();
            session.persist(newContact);
            session.getTransaction().commit();
        }
    }

    @Test
    public void should_create_contact() {
        // given
        try (SessionFactory sessionFactory = sessionFactory()) {
            var newContact = new Contact("Jack Doe", "+80455645332");
            newContact.setType(ContactType.PERSONAL);
            newContact.setPayload(
                    """
                            {
                                "name": "Jack Doe",
                                "email": "jack.doe@example.com",
                                "phoneNumber": "+80455645332"
                            }
                            """
            );
            // when | then
            create(sessionFactory, newContact);
        }
    }

    void delete(SessionFactory sessionFactory) {
        long id = 7L;
        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();
            Contact contactToDelete = session.find(Contact.class, id);
            if (contactToDelete != null) {
                session.remove(contactToDelete);
            }
            session.getTransaction().commit();
        }
    }

    static void update(SessionFactory sessionFactory) {
        Contact newContact = new Contact("John Doe", "123-456-7890");
        if (newContact.getId() != null) {
            try (Session session = sessionFactory.openSession()) {
                session.beginTransaction();
                Contact contactToUpdate = session.find(Contact.class, newContact.getId());
                if (contactToUpdate != null) {
                    contactToUpdate.setPhoneNumber("111-222-3333");
                    session.merge(contactToUpdate);
                }
                session.getTransaction().commit();
            }
        }
    }

    // Code generated by the annotation processor
    @StaticMetamodel(Contact.class)
    public class Contact_ {
        public static volatile SingularAttribute<Contact, String> name;
    }

    public List<Contact> find(Session session) {
        return SelectionSpecification.create(Contact.class, """ 
                        from Contact
                        """)
                .restrict(Restriction.startsWith(Contact_.name, "Mc"))
                .sort(Order.desc(Contact_.name))
                .createQuery(session)
                .setPage(Page.first(50))
                .getResultList();
    }

    void delete(Session session) {
        MutationSpecification.create(Contact.class, """ 
                        delete Contact
                        """)
                .restrict(Restriction.startsWith(Contact_.name, "Mc"))
                .createQuery(session)
                .executeUpdate();
    }
}
